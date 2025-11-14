# app/fetch_bank_data.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Iterable, Dict, Any, Optional

import requests

from app.env_loader import load_settings
from app.logging_setup import setup_logging
from app.db import get_conn

log = logging.getLogger("fetch")


# ─────────────────────────────── utils ───────────────────────────────

def _parse_dt(value: Optional[str], fmt: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, fmt)
    except Exception as e:
        log.debug("Datetime parse failed: %s | raw=%s | fmt=%s", e, value, fmt)
        return None


def _parse_d(value: Optional[str], fmt: str) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, fmt).date()
    except Exception as e:
        log.debug("Date parse failed: %s | raw=%s | fmt=%s", e, value, fmt)
        return None


def _ascii_headers(h: dict[str, str]) -> dict[str, str]:
    """
    Повертає копію headers з гарантовано latin-1 значеннями.
    Якщо щось не кодується в latin-1 — вирізаємо.
    """
    out: dict[str, str] = {}
    for k, v in h.items():
        sv = str(v)
        try:
            sv.encode("latin-1")
        except UnicodeEncodeError:
            sv = sv.encode("latin-1", "ignore").decode("latin-1")
        out[str(k)] = sv
    return out


# ─────────────────────────────── DB I/O ───────────────────────────────

def _get_active_accounts() -> list[dict]:
    """
    Тягнемо активні рахунки з конфіг-таблиць.
    days_back_default береться з env (BANK_DAYS_BACK).
    """
    sql = """
    SELECT a.iban,
           f.alias,
           f.env_token_key,
           COALESCE(NULLIF(%s, ''), %s) AS days_back_default
    FROM bnk_cfg_accounts a
    JOIN bnk_cfg_fops f ON f.id = a.fop_id
    WHERE a.is_active=1 AND f.is_active=1 AND a.bank='PRIVAT'
    """
    with get_conn() as cn, cn.cursor(dictionary=True) as cur:
        s = load_settings()
        cur.execute(sql, (s.__dict__.get("bank_days_back", None),
                          s.__dict__.get("bank_days_back", None)))
        return cur.fetchall()


def _get_last_tran_date(iban: str) -> Optional[date]:
    sql = "SELECT MAX(DATE(DATE_TIME_DAT_OD_TIM_P)) FROM bnk_trazact_prvt WHERE AUT_MY_ACC=%s"
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, (iban,))
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def _insert_or_update_raw(rows: Iterable[Dict[str, Any]]) -> tuple[int, int, int]:
    """
    Пише прямо в bnk_trazact_prvt. ON DUPLICATE за PK таблиці.
    Повертає (insert, update, skip).
    """
    rows = list(rows)
    if not rows:
        return (0, 0, 0)

    sql = """
    INSERT INTO bnk_trazact_prvt
      (NUM_DOC, UETR, DATE_TIME_DAT_OD_TIM_P, DAT_OD, AUT_MY_CRF, SUM_E, CCY,
       AUT_CNTR_NAM, AUT_MY_MFO_CITY, AUT_CNTR_CRF, DOC_TYP, SUM, AUT_CNTR_MFO,
       OSND, TIM_P, STRUCT_CODE, AUT_CNTR_ACC, REFN, AUT_MY_MFO, PR_PR, DLR,
       AUT_CNTR_MFO_CITY, ULTMT, FL_REAL, AUT_MY_NAM, AUT_MY_MFO_NAME, REF,
       DAT_KL, ID, AUT_MY_ACC, TRANTYPE, TECHNICAL_TRANSACTION_ID,
       AUT_CNTR_MFO_NAME, PAYER_ULTMT_NCEO, PAYER_ULTMT_NAME)
    VALUES
      (%(NUM_DOC)s, %(UETR)s, %(DATE_TIME_DAT_OD_TIM_P)s, %(DAT_OD)s, %(AUT_MY_CRF)s, %(SUM_E)s, %(CCY)s,
       %(AUT_CNTR_NAM)s, %(AUT_MY_MFO_CITY)s, %(AUT_CNTR_CRF)s, %(DOC_TYP)s, %(SUM)s, %(AUT_CNTR_MFO)s,
       %(OSND)s, %(TIM_P)s, %(STRUCT_CODE)s, %(AUT_CNTR_ACC)s, %(REFN)s, %(AUT_MY_MFO)s, %(PR_PR)s, %(DLR)s,
       %(AUT_CNTR_MFO_CITY)s, %(ULTMT)s, %(FL_REAL)s, %(AUT_MY_NAM)s, %(AUT_MY_MFO_NAME)s, %(REF)s,
       %(DAT_KL)s, %(ID)s, %(AUT_MY_ACC)s, %(TRANTYPE)s, %(TECHNICAL_TRANSACTION_ID)s,
       %(AUT_CNTR_MFO_NAME)s, %(PAYER_ULTMT_NCEO)s, %(PAYER_ULTMT_NAME)s)
    ON DUPLICATE KEY UPDATE
      UETR=VALUES(UETR),
      SUM_E=VALUES(SUM_E),
      CCY=VALUES(CCY),
      AUT_CNTR_NAM=VALUES(AUT_CNTR_NAM),
      AUT_CNTR_CRF=VALUES(AUT_CNTR_CRF),
      SUM=VALUES(SUM),
      OSND=VALUES(OSND),
      AUT_CNTR_ACC=VALUES(AUT_CNTR_ACC),
      updated_at=CURRENT_TIMESTAMP
    """

    ins = upd = skp = 0
    logged = 0

    with get_conn() as cn, cn.cursor() as cur:
        for r in rows:
            # Мінімальна нормалізація
            if not r.get("TRANTYPE"):
                try:
                    amt = float(r.get("SUM") or 0)
                    r["TRANTYPE"] = "D" if amt < 0 else "C"
                except Exception:
                    r["TRANTYPE"] = "C"

            if r.get("AUT_CNTR_MFO") is None:
                r["AUT_CNTR_MFO"] = ""

            # Контроль PK
            if (not r.get("NUM_DOC")
                    or not r.get("DATE_TIME_DAT_OD_TIM_P")
                    or r.get("AUT_CNTR_MFO") is None
                    or not r.get("TRANTYPE")):
                if logged < 10:
                    log.warning(
                        "Skip row (missing PK fields): %s",
                        {k: r.get(k) for k in ("NUM_DOC", "DATE_TIME_DAT_OD_TIM_P", "AUT_CNTR_MFO", "TRANTYPE")}
                    )
                    logged += 1
                skp += 1
                continue

            try:
                cur.execute(sql, r)
                if cur.rowcount == 1:
                    ins += 1
                elif cur.rowcount == 2:
                    upd += 1
                else:
                    skp += 1
            except Exception as e:
                if logged < 10:
                    log.exception("Skip row due to DB error: %s", e)
                    log.debug("Row data: %s", r)
                    logged += 1
                skp += 1

        cn.commit()

    return ins, upd, skp


def _rebuild_ekv_for_range(date_from: date, date_to: date) -> None:
    """
    Перекладаємо в bnk_trazact_prvt_ekv спільні колонки за діапазоном дат.
    """
    sql = """
    INSERT INTO bnk_trazact_prvt_екv (
      NUM_DOC, UETR, DATE_TIME_DAT_OD_TIM_P, DAT_OD, AUT_MY_CRF, SUM_E, CCY,
      AUT_CNTR_NAM, AUT_MY_MFO_CITY, AUT_CNTR_CRF, DOC_TYP, SUM, AUT_CNTR_MFO,
      OSND, TIM_P, STRUCT_CODE, AUT_CNTR_ACC, REFN, AUT_MY_MFO, PR_PR, DLR,
      AUT_CNTR_MFO_CITY, ULTMT, FL_REAL, AUT_MY_NAM, AUT_MY_MFO_NAME, REF,
      DAT_KL, ID, AUT_MY_ACC, TRANTYPE, TECHNICAL_TRANSACTION_ID,
      AUT_CNTR_MFO_NAME, PAYER_ULTMT_NCEO, PAYER_ULTMT_NAME
    )
    SELECT
      NUM_DOC, UETR, DATE_TIME_DAT_OD_TIM_P, DAT_OD, AUT_MY_CRF, SUM_E, CCY,
      AUT_CNTR_NAM, AUT_MY_MFO_CITY, AUT_CNTR_CRF, DOC_TYP, SUM, AUT_CNTR_MFO,
      OSND, TIM_P, STRUCT_CODE, AUT_CNTR_ACC, REFN, AUT_MY_MFO, PR_PR, DLR,
      AUT_CNTR_MFO_CITY, ULTMT, FL_REAL, AUT_MY_NAM, AUT_MY_MFO_NAME, REF,
      DAT_KL, ID, AUT_MY_ACC, TRANTYPE, TECHNICAL_TRANSACTION_ID,
      AUT_CNTR_MFO_NAME, PAYER_ULTMT_NCEO, PAYER_ULTMT_NAME
    FROM bnk_trazact_prvt
    WHERE DATE(DATE_TIME_DAT_OD_TIM_P) BETWEEN %s AND %s
    ON DUPLICATE KEY UPDATE
      SUM = VALUES(SUM),
      OSND = VALUES(OSND),
      updated_at = CURRENT_TIMESTAMP
    """
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, (date_from, date_to))
        cn.commit()


# ─────────────────────────── Privat API fetch ───────────────────────────

def privat_fetch_statements(iban: str, d_from: date, d_to: date, token: str) -> list[dict]:
    """
    Тягнемо виписки по API. Парсимо дати одразу у date/datetime.
    ASCII-заголовки, щоб не ловити UnicodeEncodeError у http.client.
    """
    url = "https://acp.privatbank.ua/api/statements/transactions"

    # ВСЕ ASCII! Жодних «лапок», кирилиці тощо.
    headers = _ascii_headers({
        "User-Agent": "BankToEnote/1.0",
        "Accept": "application/json",
        "Content-Type": "application/json;charset=cp1251",
        "token": token,  # токен — ASCII
    })

    params = {
        "acc": iban,
        "startDate": d_from.strftime("%d-%m-%Y"),
        "endDate": d_to.strftime("%d-%m-%Y"),
        "limit": "50",
    }

    out: list[dict] = []
    next_page_id: Optional[str] = None

    while True:
        if next_page_id:
            params["followId"] = next_page_id

        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            log.error("Privat HTTP %s: %s", r.status_code, r.text)
            break

        data = r.json()
        if data.get("status") != "SUCCESS":
            log.error("Privat error: %s", data.get("message"))
            break

        txs = data.get("transactions", []) or []
        if not txs:
            log.info("No new transactions for %s", iban)
        else:
            for t in txs:
                dt_parsed = _parse_dt(t.get("DATE_TIME_DAT_OD_TIM_P"), "%d.%m.%Y %H:%M:%S")
                d_parsed  = _parse_d(t.get("DAT_OD"), "%d.%m.%Y")
                kl_parsed = _parse_d(t.get("DAT_KL"), "%d.%m.%Y")

                row = {
                    "NUM_DOC": t.get("NUM_DOC") or t.get("REF") or t.get("ID"),
                    "UETR": t.get("UETR"),
                    "DATE_TIME_DAT_OD_TIM_P": dt_parsed,
                    "DAT_OD": d_parsed,
                    "AUT_MY_CRF": t.get("AUT_MY_CRF"),
                    "SUM_E": t.get("SUM_E"),
                    "CCY": t.get("CCY"),
                    "AUT_CNTR_NAM": t.get("AUT_CNTR_NAM"),
                    "AUT_MY_MFO_CITY": t.get("AUT_MY_MFO_CITY"),
                    "AUT_CNTR_CRF": t.get("AUT_CNTR_CRF"),
                    "DOC_TYP": t.get("DOC_TYP"),
                    "SUM": t.get("SUM"),
                    "AUT_CNTR_MFO": t.get("AUT_CNTR_MFO") if t.get("AUT_CNTR_MFO") is not None else "",
                    "OSND": t.get("OSND"),
                    "TIM_P": t.get("TIM_P"),
                    "STRUCT_CODE": t.get("STRUCT_CODE"),
                    "AUT_CNTR_ACC": t.get("AUT_CNTR_ACC"),
                    "REFN": t.get("REFN"),
                    "AUT_MY_MFO": t.get("AUT_MY_MFO"),
                    "PR_PR": t.get("PR_PR"),
                    "DLR": t.get("DLR"),
                    "AUT_CNTR_MFO_CITY": t.get("AUT_CNTR_MFO_CITY"),
                    "ULTMT": t.get("ULTMT"),
                    "FL_REAL": t.get("FL_REAL"),
                    "AUT_MY_NAM": t.get("AUT_MY_NAM"),
                    "AUT_MY_MFO_NAME": t.get("AUT_MY_MFO_NAME"),
                    "REF": t.get("REF"),
                    "DAT_KL": kl_parsed,
                    "ID": t.get("ID"),
                    "AUT_MY_ACC": iban,
                    "TRANTYPE": t.get("TRANTYPE"),
                    "TECHNICAL_TRANSACTION_ID": t.get("TECHNICAL_TRANSACTION_ID"),
                    "AUT_CNTR_MFO_NAME": t.get("AUT_CNTR_MFO_NAME"),
                    "PAYER_ULTMT_NCEO": t.get("PAYER_ULTMT_NCEO"),
                    "PAYER_ULTMT_NAME": t.get("PAYER_ULTMT_NAME"),
                }

                if not row["TRANTYPE"]:
                    try:
                        amt = float(row.get("SUM") or 0)
                        row["TRANTYPE"] = "D" if amt < 0 else "C"
                    except Exception:
                        row["TRANTYPE"] = "C"

                # PK контроль
                if (not row["NUM_DOC"]
                        or not row["DATE_TIME_DAT_OD_TIM_P"]
                        or row["AUT_CNTR_MFO"] is None
                        or not row["TRANTYPE"]):
                    log.warning(
                        "Skip API row (missing PK): NUM_DOC=%s, DT=%s, MFO=%s, TT=%s",
                        row.get("NUM_DOC"), row.get("DATE_TIME_DAT_OD_TIM_P"),
                        row.get("AUT_CNTR_MFO"), row.get("TRANTYPE"),
                    )
                    continue

                out.append(row)

        if data.get("exist_next_page"):
            next_page_id = data.get("next_page_id")
            log.debug("Next page for %s: %s", iban, next_page_id)
        else:
            break

    return out


# ───────────────────── еквайринг — рядки комісій (_ek) ─────────────────────

import re
def create_acquiring_commission_rows():
    sql_select = """
        SELECT *
        FROM bnk_trazact_prvt_ekv
        WHERE TRANTYPE='C'
          AND AUT_CNTR_NAM LIKE 'Розрахунки з еквайринг%'
          AND OSND REGEXP 'Ком[[:space:]]*бан[[:space:]]*[0-9]'
    """
    sql_insert = """
        INSERT INTO bnk_trazact_prvt_ekв (
            NUM_DOC, UETR, DATE_TIME_DAT_OD_TIM_P, DAT_OD, AUT_MY_CRF, SUM_E, CCY,
            AUT_CNTR_NAM, AUT_MY_MFO_CITY, AUT_CNTR_CRF, DOC_TYP, SUM, AUT_CNTR_MFO,
            OSND, TIM_P, STRUCT_CODE, AUT_CNТР_ACC, REFN, AUT_MY_MFO, PR_PR, DLR,
            AUT_CNTR_MFO_CITY, ULTMT, FL_REAL, AUT_MY_NAM, AUT_MY_MFO_NAME, REF,
            DAT_KL, ID, AUT_MY_ACC, TRANTYPE, TECHNICAL_TRANSACTION_ID,
            AUT_CNTR_MFO_NAME, created_at, updated_at
        )
        VALUES (
            %(NUM_DOC)s, %(UETR)s, %(DATE_TIME_DAT_OD_TIM_P)s, %(DAT_OD)s, %(AUT_MY_CRF)s,
            %(SUM_E)s, %(CCY)s, %(AUT_CNTR_NAM)s, %(AUT_MY_MFO_CITY)s, %(AUT_CNTR_CRF)s,
            %(DOC_TYP)s, %(SUM)s, %(AUT_CNTR_MFO)s, %(OSND)s, %(TIM_P)s, %(STRUCT_CODE)s,
            %(AUT_CNTR_ACC)s, %(REFN)s, %(AUT_MY_MFO)s, %(PR_PR)s, %(DLR)s,
            %(AUT_CNTR_MFO_CITY)s, %(ULTMT)s, %(FL_REAL)s, %(AUT_MY_NAM)s,
            %(AUT_MY_MFO_NAME)s, %(REF)s, %(DAT_KL)s, %(ID)s, %(AUT_MY_ACC)s,
            %(TRANTYPE)s, %(TECHNICAL_TRANSACTION_ID)s, %(AUT_CNTR_MFO_NAME)s,
            NOW(), NOW()
        )
        ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)
    """

    with get_conn() as cn, cn.cursor(dictionary=True) as cur:
        cur.execute(sql_select)
        rows = cur.fetchall()

        if not rows:
            log.info("No base rows found for acquiring commissions.")
            return

        inserted = 0
        for r in rows:
            m = re.search(r"Ком\s*бан\s*([0-9]+[.,]?[0-9]*)", r.get("OSND", ""), flags=re.IGNORECASE)
            if not m:
                continue
            fee = float(m.group(1).replace(",", "."))
            new_row = dict(r)
            new_row["NUM_DOC"] = f"{r['NUM_DOC']}_ek"
            new_row["SUM"] = round(fee, 2)
            new_row["SUM_E"] = new_row["SUM"]
            new_row["TRANTYPE"] = "D"
            new_row["OSND"] = "Розрахунки з еквайрингом"
            try:
                cur.execute(sql_insert, new_row)
                inserted += 1
            except Exception as e:
                log.warning("Skip insert _ek for %s: %s", r["NUM_DOC"], e)

        cn.commit()
        log.info("Created %s commission (_ek) rows for acquiring.", inserted)


# ─────────────────────────────── runner ───────────────────────────────

def run() -> None:
    s = load_settings()
    setup_logging(s.log_level, s.log_file)
    log.info("Start fetch_bank_data")

    accounts = _get_active_accounts()
    if not accounts:
        log.warning("No active Privat accounts in config.")
        return

    total_ins = total_upd = total_skp = 0
    min_date: Optional[date] = None
    max_date: Optional[date] = None
    today = date.today()

    for acc in accounts:
        iban = (acc["iban"] or "").replace(" ", "")
        alias = acc["alias"]
        env_key = acc["env_token_key"]

        token = (getattr(s, "privat_tokens", {}) or {}).get(alias) or s.__dict__.get(env_key)
        if not token:
            exp = f"PRIVAT_TOKEN_{alias}"
            log.error("No token in .env for alias=%s (expected %s or %s)", alias, exp, env_key)
            continue

        try:
            acc_days_back = int(acc.get("days_back_default")) if acc.get("days_back_default") is not None else None
        except Exception:
            acc_days_back = None
        days_back = acc_days_back if acc_days_back is not None else int(getattr(s, "bank_days_back", 7) or 7)

        last = _get_last_tran_date(iban)
        d_from = (last + timedelta(days=1)) if last else (today - timedelta(days=days_back))
        d_to = today

        log.info("Pulling %s (%s) IBAN=%s from %s to %s", alias, env_key, iban, d_from, d_to)

        try:
            rows = privat_fetch_statements(iban, d_from, d_to, token)
        except Exception as e:
            log.error("Fetch failed for %s: %s", iban, e)
            continue

        ins, upd, skp = _insert_or_update_raw(rows)
        log.info("Loaded: inserted=%s updated=%s skipped=%s for %s", ins, upd, skp, iban)
        total_ins += ins
        total_upd += upd
        total_skp += skp

        if rows:
            min_date = min(min_date or d_to, d_from)
            max_date = max(max_date or d_from, d_to)

    if min_date and max_date:
        _rebuild_ekv_for_range(min_date, max_date)
        log.info("EKV updated for range: %s..%s", min_date, max_date)
        create_acquiring_commission_rows()

    log.info("Done fetch_bank_data. total inserted=%s updated=%s skipped=%s", total_ins, total_upd, total_skp)


if __name__ == "__main__":
    run()
