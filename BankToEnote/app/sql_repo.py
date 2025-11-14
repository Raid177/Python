# sql_repo.py

from __future__ import annotations
from typing import Any
import logging
from app.db import get_conn
log = logging.getLogger("sql_repo")

def fetch_ready_rows(limit: int) -> list[dict]:
    sql = "SELECT * FROM bnk_v_ready_to_post ORDER BY DATE_TIME_DAT_OD_TIM_P LIMIT %s"
    with get_conn() as cn, cn.cursor(dictionary=True) as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()

def map_our_account_to_enote_ref(aut_my_acc: str) -> str | None:
    sql = """
        SELECT Ref_Key
        FROM bnk_v_accounts_norm
        WHERE acc_clean = UPPER(TRIM(REPLACE(REPLACE(REPLACE(%s,' ',''), '\t',''), '\n','')))
        LIMIT 1
    """
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, (aut_my_acc,))
        row = cur.fetchone()
        return row[0] if row else None

def find_counterparty(edrpou_digits: str | None, iban_clean: str | None, name_norm: str | None) -> tuple[str | None, str | None]:
    with get_conn() as cn, cn.cursor() as cur:
        if edrpou_digits:
            cur.execute("SELECT Ref_Key FROM bnk_v_counterparties_norm WHERE edrpou_digits=%s LIMIT 2", (edrpou_digits,))
            rows = cur.fetchall()
            if len(rows) == 1: return rows[0][0], None
            if len(rows) > 1:  return None, "ambiguous_counterparty"
        if iban_clean:
            cur.execute("SELECT Ref_Key FROM bnk_v_counterparties_norm WHERE iban1_clean=%s OR iban2_clean=%s LIMIT 2", (iban_clean,iban_clean))
            rows = cur.fetchall()
            if len(rows) == 1: return rows[0][0], None
            if len(rows) > 1:  return None, "ambiguous_counterparty"
        if name_norm:
            cur.execute("SELECT Ref_Key FROM bnk_v_counterparties_norm WHERE name_norm=%s LIMIT 2", (name_norm,))
            rows = cur.fetchall()
            if len(rows) == 1: return rows[0][0], None
            if len(rows) > 1:  return None, "ambiguous_counterparty"
    return None, "counterparty_not_found"

def touch_try(pk: tuple[str,str,str,str]) -> None:
    sql = """UPDATE bnk_trazact_prvt_ekv
             SET try_count=try_count+1, ts_last_try=NOW()
             WHERE NUM_DOC=%s AND DATE_TIME_DAT_OD_TIM_P=%s AND AUT_CNTR_MFO=%s AND TRANTYPE=%s"""
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, pk)

def mark_error(pk: tuple[str,str,str,str], code: str, msg: str | None) -> None:
    sql = """UPDATE bnk_trazact_prvt_ekv
             SET err_code=%s, err_msg=%s, ts_last_try=NOW(), try_count=try_count+1
             WHERE NUM_DOC=%s AND DATE_TIME_DAT_OD_TIM_P=%s AND AUT_CNTR_MFO=%s AND TRANTYPE=%s"""
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, (code, msg, *pk))

def mark_posted(pk: tuple[str,str,str,str], enote_ref: str, enote_number: str | None) -> None:
    sql = """UPDATE bnk_trazact_prvt_ekv
             SET enote_ref=%s, enote_check=%s, ts_last_try=NOW(), try_count=try_count+1
             WHERE NUM_DOC=%s AND DATE_TIME_DAT_OD_TIM_P=%s AND AUT_CNTR_MFO=%s AND TRANTYPE=%s"""
    # sql = sql.replace("НТР","")  # захист від кирилиці в копіпасті
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, (enote_ref, enote_number, *pk))
