# app/enote_push.py
from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

from app.db import get_conn
from app.env_loader import load_settings

log = logging.getLogger("enote_push")

# ───────────────────────────── helpers ─────────────────────────────

def _od_base_url(s) -> str:
    env = (getattr(s, "enote_env", None) or os.getenv("ENOTE_ENV", "prod")).strip().lower()
    if env == "copy":
        base = getattr(s, "odata_url_copy", None) or os.getenv("ODATA_URL_COPY")
    else:
        base = getattr(s, "odata_url", None) or os.getenv("ODATA_URL")
    if not base:
        raise RuntimeError("ODATA_URL/ODATA_URL_COPY is not configured")
    return base.rstrip("/")

def _od_creds(s) -> tuple[str, str]:
    user = getattr(s, "odata_user", None) or os.getenv("ODATA_USER")
    pwd  = getattr(s, "odata_password", None) or os.getenv("ODATA_PASSWORD")
    if not user or not pwd:
        raise RuntimeError("ODATA_USER/ODATA_PASSWORD is not configured")
    return user, pwd

def _guid_env(name: str, s) -> str:
    v = getattr(s, name.lower(), None) or os.getenv(name)
    if not v:
        raise RuntimeError(f"{name} is not configured")
    return v

def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if raw == "":
        return default
    return raw in ("1", "true", "yes", "on")

def _env_int_pos(name: str) -> int | None:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
        return n if n > 0 else None
    except Exception:
        return None

def _global_dry_run() -> bool:
    raw = (os.getenv("DRY_RUN", "") or "").strip().lower()
    return raw not in ("", "0", "false", "no", "off")

def _post_enote_document(base_url: str, auth: HTTPBasicAuth, payload: dict) -> requests.Response:
    url = f"{base_url}/Document_ДенежныйЧек"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    return requests.post(url, headers=headers, auth=auth, json=payload, timeout=60)

def _post_enote_post_action(base_url: str, auth: HTTPBasicAuth, ref_key: str) -> requests.Response:
    url = f"{base_url}/Document_ДенежныйЧек(guid'{ref_key}')/Post"
    params = {"PostingModeOperational": "false"}
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    return requests.post(url, headers=headers, auth=auth, params=params, timeout=60)

# ───────────────────────────── main ─────────────────────────────

def push_to_enote(limit: Optional[int] = None, dry_run: Optional[bool] = None) -> None:
    """
    Беремо з bnk_trazact_prvt_ekv всі TRANTYPE='D' без enote_ref/err_code.
    Створюємо Document_ДенежныйЧек і проводимо.
    DRY_RUN (глобальний, з .env): нічого не шле і не змінює БД.
    """
    s = load_settings()
    base_url = _od_base_url(s)
    user, pwd = _od_creds(s)
    auth = HTTPBasicAuth(user, pwd)

    # GUID-и за замовчанням
    CURRENCY = _guid_env("DEFAULT_CURRENCY_GUID_UAH", s)
    ORG      = _guid_env("DEFAULT_ORG_GUID", s)
    SUBDIV   = _guid_env("DEFAULT_SUBDIVISION_GUID", s)
    RESP     = _guid_env("DEFAULT_RESPONSIBLE_GUID", s)

    if dry_run is None:
        dry_run = _global_dry_run()

    # ліміт з .env, якщо не передано аргументом
    if limit is None:
        limit = _env_int_pos("ENOTE_PUSH_LIMIT")

    log.info("ENOTE_ENV=%s | base=%s | DRY_RUN=%s | LIMIT=%s",
             (getattr(s, 'enote_env', None) or os.getenv('ENOTE_ENV', 'prod')),
             base_url, dry_run, (limit if limit is not None else "∞"))

    # 1) Кандидати
    sql_sel = """
        SELECT *
        FROM bnk_trazact_prvt_ekv
        WHERE TRANTYPE='D'
          AND (enote_ref IS NULL OR enote_ref = '')
          AND (err_code IS NULL OR err_code = '')
        ORDER BY DATE_TIME_DAT_OD_TIM_P ASC
    """
    if limit is not None:
        sql_sel += f" LIMIT {int(limit)}"

    # 2) Пошук рахунку/контрагента
    sql_find_acc = "SELECT Ref_Key FROM et_Catalog_ДенежныеСчета WHERE НомерСчета=%s"
    sql_find_cnt_inn    = "SELECT Ref_Key FROM et_x_Catalog_Контрагенты WHERE ИНН=%s"
    sql_find_cnt_edrpou = "SELECT Ref_Key FROM et_x_Catalog_Контрагенты WHERE ЕДРПОУ=%s"

    # 3) Маркування
    sql_mark_err = """
        UPDATE bnk_trazact_prvt_ekv
        SET err_code=%s, err_msg=%s, try_count=COALESCE(try_count,0)+1, ts_last_try=NOW()
        WHERE NUM_DOC=%s AND DATE_TIME_DAT_OD_TIM_P=%s AND AUT_CNTR_MFO=%s AND TRANTYPE=%s
    """
    sql_mark_ok = """
        UPDATE bnk_trazact_prvt_ekv
        SET enote_ref=%s, enote_check=%s, err_code=NULL, err_msg=NULL,
            try_count=COALESCE(try_count,0)+1, ts_last_try=NOW()
        WHERE NUM_DOC=%s AND DATE_TIME_DAT_OD_TIM_P=%s AND AUT_CNTR_MFO=%s AND TRANTYPE=%s
    """

    with get_conn() as cn, cn.cursor(dictionary=True) as cur:
        cur.execute(sql_sel)
        rows = cur.fetchall()

        if not rows:
            log.info("push_to_enote: nothing to push.")
            return

        done, errs = 0, 0

        for r in rows:
            num_doc   = r["NUM_DOC"]
            dt        = r["DATE_TIME_DAT_OD_TIM_P"]
            aut_mfo   = r["AUT_CNTR_MFO"]
            trantype  = r["TRANTYPE"]
            aut_myacc = r["AUT_MY_ACC"]

            tax = (str(r.get("AUT_CNTR_INN") or r.get("AUT_CNTR_CRF") or "")).strip()
            aut_cnt_n = (r.get("AUT_CNTR_NAM") or "").strip()

            amount = float(r["SUM"] or 0.0)
            osnd   = r.get("OSND") or ""
            dat_od = r.get("DAT_OD")

            # рахунок
            cur.execute(sql_find_acc, (aut_myacc,))
            acc = cur.fetchone()
            if not acc:
                msg = f"Немає рахунку в et_Catalog_ДенежныеСчета для {aut_myacc}"
                if dry_run:
                    log.warning("DRY-RUN NO_ACC: %s | %s", num_doc, msg)
                else:
                    cur.execute(sql_mark_err, ("NO_ACC", msg[:1024], num_doc, dt, aut_mfo, trantype))
                    cn.commit()
                errs += 1
                continue
            acc_ref = acc["Ref_Key"]

            # контрагент
            cnt = None
            if tax:
                cur.execute(sql_find_cnt_inn, (tax,))
                cnt = cur.fetchone()
                if not cnt:
                    cur.execute(sql_find_cnt_edrpou, (tax,))
                    cnt = cur.fetchone()

            if not cnt:
                msg = (f"Немає контрагента для tax='{tax}' "
                       f"(INN/ЄДРПОУ у et_x_Catalog_Контрагенты); name='{aut_cnt_n}'")
                if dry_run:
                    log.warning("DRY-RUN NO_CNT: %s | %s", num_doc, msg)
                else:
                    cur.execute(sql_mark_err, ("NO_CNT", msg[:1024], num_doc, dt, aut_mfo, trantype))
                    cn.commit()
                errs += 1
                continue
            cnt_ref = cnt["Ref_Key"]

            # документ
            dt_iso = (dt if isinstance(dt, datetime) else datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%S")
            payload = {
                "Date": dt_iso,
                "Posted": True,
                "Валюта_Key": CURRENCY,
                "ВидДвижения": "РасчетыСПоставщиками",
                "ДенежныйСчет": acc_ref,
                "ДенежныйСчетБезнал_Key": acc_ref,
                "ДенежныйСчет_Type": "StandardODATA.Catalog_ДенежныеСчета",
                "Комментарий": f"Racoon_BankTransaction, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{osnd}",
                "Кратность": "1",
                "Курс": 1,
                "НаправлениеДвижения": "Расход",
                "Объект": cnt_ref,
                "Объект_Type": "StandardODATA.Catalog_Контрагенты",
                "Организация_Key": ORG,
                "Ответственный_Key": RESP,
                "Подразделение_Key": SUBDIV,
                "Сумма": 0,
                "СуммаБезнал": amount,
            }

            if dry_run:
                log.info(
                    "DRY-RUN: створив би Document_ДенежныйЧек (%.2f грн) | %s | %s | %s",
                    amount, (dat_od or ""), aut_cnt_n, num_doc
                )
                log.debug("DRY-RUN payload: %s", payload)
                continue

            # POST створення
            try:
                resp = _post_enote_document(base_url, auth, payload)
            except Exception as e:
                cur.execute(sql_mark_err, ("HTTP_EXC", f"{type(e).__name__}: {e}", num_doc, dt, aut_mfo, trantype))
                cn.commit()
                errs += 1
                continue

            if resp.status_code not in (200, 201):
                cur.execute(sql_mark_err, (f"HTTP_{resp.status_code}", resp.text[:1024],
                                           num_doc, dt, aut_mfo, trantype))
                cn.commit()
                errs += 1
                continue

            try:
                data = resp.json()
            except Exception:
                data = {}

            ref_key = (data or {}).get("Ref_Key")
            number  = (data or {}).get("Number")
            if not ref_key:
                cur.execute(sql_mark_err, ("NO_REF", f"Відповідь без Ref_Key: {resp.text[:1024]}",
                                           num_doc, dt, aut_mfo, trantype))
                cn.commit()
                errs += 1
                continue

            # POST проведення (не фейлимо цикл, якщо не вдалось)
            try:
                post_resp = _post_enote_post_action(base_url, auth, ref_key)
                if post_resp.status_code not in (200, 204):
                    log.warning("Post failed for %s: %s %s", ref_key, post_resp.status_code, post_resp.text)
            except Exception as e:
                log.warning("Post exception for %s: %s", ref_key, e)

            # OK
            cur.execute(sql_mark_ok, (ref_key, number, num_doc, dt, aut_mfo, trantype))
            cn.commit()
            done += 1

        log.info("push_to_enote: done=%s, errors=%s%s",
                 done, errs, " (DRY-RUN)" if dry_run else "")
