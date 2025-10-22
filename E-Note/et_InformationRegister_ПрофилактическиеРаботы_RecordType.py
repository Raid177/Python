#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL з OData -> MariaDB:
InformationRegister_ПрофилактическиеРаботы_RecordType

Вимоги:
  pip install python-dotenv requests mysql-connector-python

.env (приклад):
  ODATA_URL=https://app.enote.vet/xxxx/odata/standard.odata/
  ODATA_USER=odata
  ODATA_PASSWORD=***
  DB_HOST=127.0.0.1
  DB_PORT=3307
  DB_USER=olexii_raid
  DB_PASSWORD=***
  DB_DATABASE=petwealth
"""

import os
import time
import math
import datetime as dt
from urllib.parse import urlencode
import requests
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import requests

# -------------------- НАЛАШТУВАННЯ --------------------
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", 1000))
SLEEP_SECONDS  = float(os.getenv("SLEEP_SECONDS", 1.0))
DAYS_BACK      = int(os.getenv("DAYS_BACK", 45))
START_IF_EMPTY = os.getenv("START_IF_EMPTY", "2024-10-01T00:00:00")  # якщо таблиця порожня

TABLE = "et_InformationRegister_ПрофилактическиеРаботы_RecordType"
ODATA_ENTITY = "InformationRegister_ПрофилактическиеРаботы_RecordType"

# Порядок полів = порядок у INSERT
FIELDS = [
    "Period",
    "Recorder",
    "Recorder_Type",
    "LineNumber",
    "Active",
    "Карточка_Key",
    "ПрофилактическаяРабота_Key",
    "Номенклатура_Key",
    "Серия_Key",
    "ДатаОкончания",
    "СрокГодности",
]

# -------------------- ХЕЛПЕРИ --------------------
def parse_dt(value: str):
    """Перетворити 'YYYY-MM-DDTHH:MM:SS' -> 'YYYY-MM-DD HH:MM:SS' або None."""
    if value in (None, "", "0001-01-01T00:00:00"):
        return None
    try:
        # Деякі OData можуть давати з мс: '...:00.000'
        value = value.rstrip("Z")
        if "." in value:
            base, _ms = value.split(".", 1)
            value = base
        return dt.datetime.fromisoformat(value.replace("Z","").replace("T", " "))
    except Exception:
        return None

def fmt_dt_sql(d: dt.datetime | None):
    return d.strftime("%Y-%m-%d %H:%M:%S") if d else None

def get_env():
    load_dotenv()
    cfg = {
        "ODATA_URL": os.getenv("ODATA_URL", "").rstrip("/") + "/",
        "ODATA_USER": os.getenv("ODATA_USER"),
        "ODATA_PASSWORD": os.getenv("ODATA_PASSWORD"),
        "DB_HOST": os.getenv("DB_HOST", "127.0.0.1"),
        "DB_PORT": int(os.getenv("DB_PORT", "3306")),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_DATABASE": os.getenv("DB_DATABASE"),
    }
    missing = [k for k, v in cfg.items() if v in (None, "")]
    if missing:
        raise RuntimeError(f"В .env відсутні змінні: {', '.join(missing)}")
    return cfg

def connect_db(cfg):
    return mysql.connector.connect(
        host=cfg["DB_HOST"],
        port=cfg["DB_PORT"],
        user=cfg["DB_USER"],
        password=cfg["DB_PASSWORD"],
        database=cfg["DB_DATABASE"],
        autocommit=False,
    )

def get_window_start(conn):
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(Period) FROM {TABLE}")
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        last: dt.datetime = row[0]
        start = last - dt.timedelta(days=DAYS_BACK)
        print(f"[INFO] MAX(Period)={last} -> стартове вікно: {start} (DAYS_BACK={DAYS_BACK})")
        return start
    # якщо немає даних
    start = parse_dt(START_IF_EMPTY)
    if not start:
        start = dt.datetime(2024, 1, 1)
    print(f"[INFO] Таблиця порожня -> старт з {start} (START_IF_EMPTY)")
    return start

def build_odata_url(base, since: dt.datetime, top: int, skip: int):
    iso = since.strftime("%Y-%m-%dT%H:%M:%S")
    filter_str = f"Period ge datetime'{iso}'"
    select_str = ",".join(FIELDS)
    orderby_str = "Period,Recorder,LineNumber"

    # збираємо руками — без urlencode, бо OData Enote не любить %xx
    url = (
        f"{base}{ODATA_ENTITY}"
        f"?$format=json"
        f"&$orderby={orderby_str}"
        f"&$select={select_str}"
        f"&$filter={filter_str}"
        f"&$top={top}"
        f"&$skip={skip}"
    )
    return url

def fetch_batch(session: requests.Session, url: str):
    r = session.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    # стандартно OData -> {"value": [...]}
    return data.get("value", data.get("d", {}).get("results", []))

def normalize_row(item: dict):
    # Звести до кортежа значень у порядку FIELDS
    m = {}
    # Перетворення дат
    m["Period"] = fmt_dt_sql(parse_dt(item.get("Period")))
    m["Recorder"] = item.get("Recorder")
    m["Recorder_Type"] = item.get("Recorder_Type")
    # LineNumber в БД INT -> приводимо
    ln = item.get("LineNumber")
    try:
        ln = int(ln) if ln is not None else None
    except Exception:
        ln = None
    m["LineNumber"] = ln
    # Active -> bool -> tinyint
    act = item.get("Active")
    if isinstance(act, bool):
        m["Active"] = 1 if act else 0
    else:
        m["Active"] = 1 if str(act).lower() in ("1", "true", "t", "yes") else 0

    m["Карточка_Key"] = item.get("Карточка_Key")
    m["ПрофилактическаяРабота_Key"] = item.get("ПрофилактическаяРабота_Key")
    m["Номенклатура_Key"] = item.get("Номенклатура_Key")
    m["Серия_Key"] = item.get("Серия_Key")

    m["ДатаОкончания"] = fmt_dt_sql(parse_dt(item.get("ДатаОкончания")))
    m["СрокГодности"]  = fmt_dt_sql(parse_dt(item.get("СрокГодности")))
    # повертаємо кортеж у порядку FIELDS
    return tuple(m.get(k) for k in FIELDS)

def upsert_batch(conn, rows):
    if not rows:
        return (0, 0)
    cols = ", ".join(f"`{c}`" for c in FIELDS)
    placeholders = ", ".join(["%s"] * len(FIELDS))
    # ON DUPLICATE: оновлюємо всі, крім PK; updated_at = CURRENT_TIMESTAMP
    upd_cols = [c for c in FIELDS if c not in ("Recorder", "LineNumber")]
    set_clause = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in upd_cols)
    sql = f"""
        INSERT INTO `{TABLE}` ({cols})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
            {set_clause},
            `updated_at`=CURRENT_TIMESTAMP
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    # Після executemany rowcount містить суму affected rows (insert=1, update=2)
    affected = cur.rowcount
    conn.commit()
    cur.close()
    # Оцінимо приблизні вставки/оновлення (евристика)
    # якщо всі вставки: affected == n
    # якщо всі оновлення: affected == 2n
    n = len(rows)
    # обмежимо оцінки в коректних межах
    est_updates = max(0, min(n, affected - n))
    est_inserts = n - est_updates
    return (est_inserts, est_updates)

# -------------------- MAIN --------------------
def main():
    cfg = get_env()

    # HTTP сесія
    session = requests.Session()
    session.auth = (cfg["ODATA_USER"], cfg["ODATA_PASSWORD"])
    session.headers.update({"Accept": "application/json"})

    # DB
    conn = connect_db(cfg)

    try:
        since = get_window_start(conn)
        print(f"[START] BATCH_SIZE={BATCH_SIZE}, SLEEP_SECONDS={SLEEP_SECONDS}, WINDOW_FROM={since}")

        skip = 0
        total_read = 0
        total_ins = 0
        total_upd = 0
        page = 0

        while True:
            url = build_odata_url(cfg["ODATA_URL"], since, BATCH_SIZE, skip)
            page += 1
            print(f"[FETCH] page={page} skip={skip} url={url}")
            try:
                items = fetch_batch(session, url)
            except requests.HTTPError as e:
                print(f"[HTTP ERROR] {e} ; body={getattr(e.response, 'text', '')[:500]}")
                break

            if not items:
                print("[FETCH] Порожньо. Завершую.")
                break

            rows = [normalize_row(x) for x in items]
            ins, upd = upsert_batch(conn, rows)

            total_read += len(items)
            total_ins  += ins
            total_upd  += upd

            # Діапазон Part для логів
            try:
                p0 = parse_dt(items[0].get("Period"))
                p1 = parse_dt(items[-1].get("Period"))
                print(f"[BATCH] {len(items)} рядків | Period {p0} .. {p1} | +ins≈{ins} / upd≈{upd}")
            except Exception:
                print(f"[BATCH] {len(items)} рядків | +ins≈{ins} / upd≈{upd}")

            if len(items) < BATCH_SIZE:
                print("[FETCH] Остання порція (менше за BATCH_SIZE).")
                break

            skip += BATCH_SIZE
            time.sleep(SLEEP_SECONDS)

        print(f"[DONE] Прочитано: {total_read} | Вставлено≈{total_ins} | Оновлено≈{total_upd}")

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
