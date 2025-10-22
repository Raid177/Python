#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL: OData -> MySQL
Catalog_Карточки  --> et_Catalog_Карточки

- Читаємо з OData пачками по BATCH_SIZE, $orderby=Ref_Key, $skip
- Вставляємо нові, оновлюємо лише якщо DataVersion змінився
- Игноруємо вкладені масиви і навігаційні *@navigationLinkUrl
- Логи у консоль
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List, Tuple

import requests
import mysql.connector as mysql
from dotenv import load_dotenv

# ----------------------------- Config -----------------------------

ENV_PATH = os.getenv("ENV_PATH", "/root/Python/.env")
load_dotenv(ENV_PATH)

ODATA_URL      = os.getenv("ODATA_URL", "").rstrip("/") + "/"
ODATA_USER     = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "1000"))
SLEEP_SECONDS  = float(os.getenv("SLEEP_SECONDS", "1"))

TABLE_NAME     = "et_Catalog_Карточки"
ODATA_ENTITY   = "Catalog_Карточки"

# Поля точно під твою таблицю (без УДАЛИТЬЗамечания)
FIELDS: List[str] = [
    "Ref_Key",
    "DataVersion",
    "DeletionMark",
    "Code",
    "Description",
    "Хозяин_Key",
    "Вид_Key",
    "Порода_Key",
    "Масть_Key",
    "ДатаРождения",
    "Пол",
    "ДатаРегистрацииКарточки",
    "Фото_Key",
    "Комментарий",
    "КонтактнаяИнформация",
    "ДатаРожденияНеточная",
    "ЛечащийВрач_Key",
    "НомерЧипа",
    "Кастрировано",
    "ЛетальныйИсход",
    "НомерДоговора",
    "ДатаДоговора",
    "ID",
    "НомерАндиаг",
    "Организация_Key",
    "Подразделение_Key",
    "НомерКлейма",
    "ЭДО",
    "Донор",
    "ГруппаКрови",
    "АгрессивноеЖивотное",
    "Подтвержден",
    "Predefined",
    "PredefinedDataName",
]

EXCLUDE_FROM_SELECT = set(["created_at", "updated_at"])

# -------------------------- Utilities ----------------------------

def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

def bool_to_int(v: Any) -> int:
    return 1 if v is True else 0

def norm_str(s: Any) -> Any:
    if s is None:
        return None
    if isinstance(s, str):
        return s
    return str(s)

def norm_dt(s: Any) -> Any:
    if not s:
        return None
    if isinstance(s, str):
        if s.startswith("0001-01-01"):
            return None
        return s.replace("T", " ")
    return None

def normalize_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    r = {}
    r["Ref_Key"]                  = norm_str(raw.get("Ref_Key"))
    r["DataVersion"]              = norm_str(raw.get("DataVersion"))
    r["DeletionMark"]             = bool_to_int(raw.get("DeletionMark"))
    r["Code"]                     = norm_str(raw.get("Code"))
    r["Description"]              = norm_str(raw.get("Description"))
    r["Хозяин_Key"]               = norm_str(raw.get("Хозяин_Key"))
    r["Вид_Key"]                  = norm_str(raw.get("Вид_Key"))
    r["Порода_Key"]               = norm_str(raw.get("Порода_Key"))
    r["Масть_Key"]                = norm_str(raw.get("Масть_Key"))
    r["ДатаРождения"]             = norm_dt(raw.get("ДатаРождения"))
    r["Пол"]                      = norm_str(raw.get("Пол"))
    r["ДатаРегистрацииКарточки"]  = norm_dt(raw.get("ДатаРегистрацииКарточки"))
    r["Фото_Key"]                 = norm_str(raw.get("Фото_Key"))
    r["Комментарий"]              = norm_str(raw.get("Комментарий"))
    r["КонтактнаяИнформация"]     = norm_str(raw.get("КонтактнаяИнформация"))
    r["ДатаРожденияНеточная"]     = bool_to_int(raw.get("ДатаРожденияНеточная"))
    r["ЛечащийВрач_Key"]          = norm_str(raw.get("ЛечащийВрач_Key"))
    r["НомерЧипа"]                = norm_str(raw.get("НомерЧипа"))
    r["Кастрировано"]             = bool_to_int(raw.get("Кастрировано"))
    r["ЛетальныйИсход"]           = bool_to_int(raw.get("ЛетальныйИсход"))
    r["НомерДоговора"]            = norm_str(raw.get("НомерДоговора"))
    r["ДатаДоговора"]             = norm_dt(raw.get("ДатаДоговора"))
    r["ID"]                       = norm_str(raw.get("ID"))
    r["НомерАндиаг"]              = norm_str(raw.get("НомерАндиаг"))
    r["Организация_Key"]          = norm_str(raw.get("Организация_Key"))
    r["Подразделение_Key"]        = norm_str(raw.get("Подразделение_Key"))
    r["НомерКлейма"]              = norm_str(raw.get("НомерКлейма"))
    r["ЭДО"]                      = bool_to_int(raw.get("ЭДО"))
    r["Донор"]                    = bool_to_int(raw.get("Донор"))
    r["ГруппаКрови"]              = norm_str(raw.get("ГруппаКрови"))
    r["АгрессивноеЖивотное"]      = bool_to_int(raw.get("АгрессивноеЖивотное"))
    r["Подтвержден"]              = bool_to_int(raw.get("Подтвержден"))
    r["Predefined"]               = bool_to_int(raw.get("Predefined"))
    r["PredefinedDataName"]       = norm_str(raw.get("PredefinedDataName"))
    return r

def dict_to_tuple(d: Dict[str, Any], cols: List[str]) -> Tuple[Any, ...]:
    return tuple(d.get(c) for c in cols)

# ------------------------- SQL Builders --------------------------

INSERT_COLUMNS = FIELDS
INSERT_PLACEHOLDERS = ", ".join(["%s"] * len(INSERT_COLUMNS))
INSERT_COLS_SQL = ", ".join(f"`{c}`" for c in INSERT_COLUMNS)

INSERT_SQL = f"""
INSERT INTO `{TABLE_NAME}` ({INSERT_COLS_SQL})
VALUES ({INSERT_PLACEHOLDERS})
"""

UPDATE_SET_SQL = ", ".join([f"`{c}`=%s" for c in FIELDS if c != "Ref_Key"])
UPDATE_SQL = f"""
UPDATE `{TABLE_NAME}`
SET {UPDATE_SET_SQL}, `updated_at`=CURRENT_TIMESTAMP
WHERE `Ref_Key`=%s
"""

# ---------------------- DB / OData helpers -----------------------

def db_connect():
    return mysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_DATABASE, autocommit=False, charset="utf8mb4", use_unicode=True
    )

def preload_versions(cur) -> Dict[str, str]:
    cur.execute(f"SELECT `Ref_Key`, `DataVersion` FROM `{TABLE_NAME}`")
    return {rk: dv for rk, dv in cur.fetchall()}

def fetch_batch(session: requests.Session, skip: int, top: int) -> List[Dict[str, Any]]:
    select_fields = ",".join([f for f in FIELDS if f not in EXCLUDE_FROM_SELECT])
    url = (
        f"{ODATA_URL}{ODATA_ENTITY}"
        f"?$select={select_fields}"
        f"&$orderby=Ref_Key"
        f"&$top={top}"
        f"&$skip={skip}"
    )
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if "value" in payload:
        return payload["value"]
    if isinstance(payload, list):
        return payload
    return []

# ------------------------------ Main -----------------------------

def main():
    for k, v in [
        ("ODATA_URL", ODATA_URL),
        ("ODATA_USER", ODATA_USER),
        ("ODATA_PASSWORD", ODATA_PASSWORD),
        ("DB_HOST", DB_HOST),
        ("DB_USER", DB_USER),
        ("DB_DATABASE", DB_DATABASE),
    ]:
        if not v:
            log(f"ERROR: env {k} is not set")
            sys.exit(2)

    log(f"Start ETL: {ODATA_ENTITY} -> {TABLE_NAME}")
    log(f"BATCH_SIZE={BATCH_SIZE}, SLEEP_SECONDS={SLEEP_SECONDS}")

    conn = db_connect()
    cur = conn.cursor()

    log("Preloading existing Ref_Key -> DataVersion ...")
    existing_versions = preload_versions(cur)
    log(f"Preloaded versions: {len(existing_versions)} rows")

    inserted_total = 0
    updated_total = 0
    skipped_total  = 0

    session = requests.Session()
    session.auth = (ODATA_USER, ODATA_PASSWORD)
    session.headers.update({"Accept": "application/json"})

    skip = 0
    batch_idx = 0

    while True:
        batch_idx += 1
        try:
            rows = fetch_batch(session, skip=skip, top=BATCH_SIZE)
        except Exception as e:
            log(f"ERROR fetch batch skip={skip}: {e}")
            break

        if not rows:
            log("No more rows. Done.")
            break

        to_insert: List[Tuple[Any, ...]] = []
        to_update: List[Tuple[Any, ...]] = []

        batch_first_key = None
        batch_last_key = None

        for raw in rows:
            n = normalize_row(raw)
            ref = n["Ref_Key"]
            if batch_first_key is None:
                batch_first_key = ref
            batch_last_key = ref

            curr_ver = n["DataVersion"]
            prev_ver = existing_versions.get(ref)

            if prev_ver is None:
                to_insert.append(dict_to_tuple(n, INSERT_COLUMNS))
                existing_versions[ref] = curr_ver
            elif prev_ver != curr_ver:
                to_update.append(tuple(n[c] for c in FIELDS if c != "Ref_Key") + (ref,))
                existing_versions[ref] = curr_ver
            else:
                skipped_total += 1

        ins_cnt = up_cnt = 0
        try:
            if to_insert:
                cur.executemany(INSERT_SQL, to_insert)
                ins_cnt = cur.rowcount
            if to_update:
                cur.executemany(UPDATE_SQL, to_update)
                up_cnt = cur.rowcount
            conn.commit()
        except Exception as e:
            conn.rollback()
            log(f"ERROR DB batch skip={skip}: {e}")
            break

        inserted_total += ins_cnt
        updated_total  += up_cnt

        log(f"[BATCH {batch_idx}] fetched={len(rows)} "
            f"| insert={ins_cnt} update={up_cnt} skip(no-change)={len(rows) - ins_cnt - len(to_update)} "
            f"| keys {batch_first_key} .. {batch_last_key}")

        if len(rows) < BATCH_SIZE:
            log("Last partial batch received — finishing.")
            break

        skip += BATCH_SIZE
        time.sleep(SLEEP_SECONDS)

    total_fetched = inserted_total + updated_total + skipped_total
    log(f"Done. Inserted={inserted_total}, Updated={updated_total}, Skipped(no-change)={skipped_total}")
    log(f"Total fetched={total_fetched} | Inserted+Updated={inserted_total + updated_total} | "
        f"Skipped={skipped_total} ({(skipped_total / total_fetched * 100 if total_fetched else 0):.1f}%)")

    try:
        cur.close()
        conn.close()
    except Exception:
        pass

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user")
