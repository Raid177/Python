# 8) runner_post.py

from __future__ import annotations
import logging
from app.env_loader import load_settings
from app.logging_setup import setup_logging
from app import sql_repo as repo
from app.enote_client import create_cashcheck, post_cashcheck
log = logging.getLogger("runner")

def _norm_name(s: str | None) -> str | None:
    if not s: return None
    return " ".join(s.upper().split())

def run():
    s = load_settings()
    setup_logging(s.log_level, s.log_file)
    log.info("Start runner_post | DRY_RUN=%s | ENV=%s", s.dry_run, s.enote_env)

    rows = repo.fetch_ready_rows(s.batch_limit)
    if not rows:
        log.info("No rows to post.")
        return

    ok, err = 0, 0
    for r in rows:
        pk = (r["NUM_DOC"], str(r["DATE_TIME_DAT_OD_TIM_P"]), r["AUT_CNTR_MFO"], r["TRANTYPE"])
        try:
            acc_ref = repo.map_our_account_to_enote_ref(r.get("AUT_MY_ACC") or "")
            if not acc_ref:
                repo.mark_error(pk, "account_not_found", f"AUT_MY_ACC={r.get('AUT_MY_ACC')}")
                err += 1; continue

            edrpou = (r.get("AUT_CNTR_CRF") or "").strip()
            edrpou_digits = "".join(ch for ch in edrpou if ch.isdigit()) or None
            iban_clean = (r.get("AUT_CNTR_ACC") or "").replace(" ","").replace("\t","").replace("\n","").upper() or None
            name_norm = _norm_name(r.get("AUT_CNTR_NAM"))

            c_ref, c_err = repo.find_counterparty(edrpou_digits, iban_clean, name_norm)
            if c_err:
                repo.mark_error(pk, c_err, f"EDRPOU={edrpou_digits}, IBAN={iban_clean}, NAME={name_norm}")
                err += 1; continue

            payload = {
                "Date": r["DATE_TIME_DAT_OD_TIM_P"].isoformat(),
                "Posted": True,
                "Валюта_Key": s.default_currency_guid_uah,
                "Организация_Key": s.default_org_guid,
                "Подразделение_Key": s.default_subdivision_guid,
                "Ответственный_Key": s.default_responsible_guid,
                "ВидДвижения": "РасчетыСПоставщиками",
                "СчетБезнал_Key": acc_ref,
                "Объект_Key": c_ref,
                "Сумма": 0,
                "СуммаБезнал": float(abs(r.get("SUM") or 0)),
                "Комментарий": f"[AUTO] NUM_DOC={r['NUM_DOC']} | {(r.get('OSND') or '')[:150]}",
            }

            if s.dry_run:
                repo.touch_try(pk); ok += 1; continue

            ref, number = create_cashcheck(payload)
            post_cashcheck(ref)
            repo.mark_posted(pk, ref, number)
            ok += 1

        except Exception as e:
            repo.mark_error(pk, "unexpected", str(e)); err += 1

    log.info("Done. OK=%s ERR=%s", ok, err)

if __name__ == "__main__":
    run()