#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, datetime as dt
from decimal import Decimal
import requests, pymysql
from dotenv import load_dotenv

LOAD_MODE = "rewrite_range"   # або "upsert_by_dataversion"
BATCH_SIZE = 1000
SLEEP_SECONDS = 1.0
DAYS_BACK = 45
START_IF_EMPTY = "2024-07-01"

TABLE  = "et_Document_ДенежныйЧек"
ENTITY = "Document_ДенежныйЧек"
AUTO_MIGRATE = False  # ← головне: не роздуваємо схему; пишемо лише в існуючі поля

def log(msg): print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")

def mysql_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def ensure_min_table(conn):
    # каркас, щоб точно були ключові поля
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
          `Ref_Key` CHAR(36) NOT NULL,
          `DataVersion` VARCHAR(50) NOT NULL,
          `DeletionMark` TINYINT(1) NOT NULL DEFAULT 0,
          `Number` VARCHAR(20) NOT NULL DEFAULT '',
          `Date` DATETIME NOT NULL,
          `Posted` TINYINT(1) NOT NULL DEFAULT 0,
          `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`Ref_Key`),
          KEY `ix_Date` (`Date`),
          KEY `ix_Number` (`Number`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
    conn.commit()

def db_columns(conn):
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{TABLE}`")
        cols = [r["Field"] for r in cur.fetchall()]
    # не чіпаємо службові
    return [c for c in cols if c not in ("created_at","updated_at")]

def get_start_date(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(`Date`) AS max_dt FROM `{TABLE}`")
        row = cur.fetchone()
    if row and row["max_dt"]:
        return (row["max_dt"] - dt.timedelta(days=DAYS_BACK)).replace(microsecond=0)
    return dt.datetime.fromisoformat(START_IF_EMPTY + "T00:00:00")

def delete_range(conn, start_dt):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM `{TABLE}` WHERE `Date` >= %s", (start_dt,))
    conn.commit()

def preload_versions(conn):
    d = {}
    with conn.cursor() as cur:
        cur.execute(f"SELECT `Ref_Key`,`DataVersion` FROM `{TABLE}`")
        for r in cur.fetchall():
            d[r["Ref_Key"]] = r["DataVersion"]
    return d

def odata_fetch_all(start_dt):
    # ВАЖЛИВО: кирилиця в шляху — не кодуємо
    base = os.getenv("ODATA_URL").rstrip("/") + f"/{ENTITY}"
    auth = (os.getenv("ODATA_USER"), os.getenv("ODATA_PASSWORD"))
    # без $select — беремо все, фільтр/сортування/пагінація залишаються
    filter_q = f"Date ge datetime'{start_dt:%Y-%m-%dT%H:%M:%S}'"
    orderby = "Date,Ref_Key"

    all_rows, skip, i = [], 0, 1
    while True:
        url = f"{base}?$format=json&$orderby={orderby}&$filter={filter_q}&$top={BATCH_SIZE}&$skip={skip}"
        resp = requests.get(url, auth=auth, timeout=120)
        if not resp.ok:
            raise RuntimeError(f"OData HTTP {resp.status_code}: {resp.text[:500]}")
        payload = resp.json()
        rows = payload.get("value", [])
        if not rows:
            log(f"[BATCH {i}] fetched=0 (done)"); break
        dts = [r.get("Date") for r in rows if r.get("Date")]
        log(f"[BATCH {i}] fetched={len(rows)} | dates {min(dts) if dts else None} .. {max(dts) if dts else None}")
        all_rows.extend(rows)
        if len(rows) < BATCH_SIZE: break
        skip += BATCH_SIZE; i += 1; time.sleep(SLEEP_SECONDS)
    return all_rows

def coerce_value(v):
    # None як є
    if v is None:
        return None
    # bool -> tinyint
    if isinstance(v, bool):
        return int(v)
    # тільки для рядків робимо датоподібну заміну T -> пробіл
    if isinstance(v, str):
        # найпростіша перевірка ISO-дат/дато-часу
        if len(v) >= 19 and "T" in v[:19]:
            return v.replace("T", " ")
        return v
    # інші типи (int/float/Decimal/...) повертаємо без змін
    return v

def filter_to_db_cols(row, cols_db):
    out = {}
    for c in cols_db:
        val = row.get(c, None)
        try:
            out[c] = coerce_value(val)
        except Exception:
            # на всяк випадок: якщо щось зовсім нетипове — пишемо як текст
            out[c] = str(val) if val is not None else None
    return out

def insert_update(conn, rows, cols):
    if not rows: return (0,0,0)
    placeholders = ", ".join(["%s"]*len(cols))
    collist = ", ".join(f"`{c}`" for c in cols)
    updset = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols if c!="Ref_Key")
    sql = f"INSERT INTO `{TABLE}` ({collist}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updset}"

    with conn.cursor() as cur:
        for i in range(0, len(rows), 1000):
            vals = [tuple(r.get(c) for c in cols) for r in rows[i:i+1000]]
            cur.executemany(sql, vals)
    conn.commit()
    return (len(rows), 0, 0)

def upsert_by_dv(conn, rows, cols):
    if not rows: return (0,0,0)
    existing = preload_versions(conn)
    to_write, ins, upd, skip = [], 0, 0, 0
    for r in rows:
        ref = r.get("Ref_Key"); dv = r.get("DataVersion")
        if not ref:
            continue
        if ref in existing:
            if existing[ref] == dv: skip += 1; continue
            upd += 1
        else:
            ins += 1
        to_write.append(tuple(r.get(c) for c in cols))
    if to_write:
        placeholders = ", ".join(["%s"]*len(cols))
        collist = ", ".join(f"`{c}`" for c in cols)
        updset = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols if c!="Ref_Key")
        sql = f"INSERT INTO `{TABLE}` ({collist}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updset}"
        with conn.cursor() as cur:
            for i in range(0, len(to_write), 1000):
                cur.executemany(sql, to_write[i:i+1000])
        conn.commit()
    return (ins, upd, skip)

def main():
    load_dotenv()
    conn = mysql_conn()
    try:
        ensure_min_table(conn)
        cols_db = db_columns(conn)  # ← беремо актуальний список полів з БД

        start_dt = get_start_date(conn)
        log(f"Start ETL: {ENTITY} -> {TABLE}")
        log(f"BATCH_SIZE={BATCH_SIZE}, SLEEP_SECONDS={SLEEP_SECONDS}, DAYS_BACK={DAYS_BACK}, LOAD_MODE={LOAD_MODE}")
        log(f"Start date (filter): {start_dt}")

        if LOAD_MODE == "rewrite_range":
            log("Deleting existing records in range ...")
            delete_range(conn, start_dt)

        raw = odata_fetch_all(start_dt)
        log(f"Total fetched: {len(raw)}")

        # фільтруємо кожний рядок до колонок БД
        trimmed = [ filter_to_db_cols(r, cols_db) for r in raw ]

        if LOAD_MODE == "rewrite_range":
            ins, upd, skip = insert_update(conn, trimmed, cols_db)
        else:
            ins, upd, skip = upsert_by_dv(conn, trimmed, cols_db)

        log(f"Done. inserted={ins} updated={upd} skipped={skip}")
    finally:
        try: conn.close()
        except: pass

if __name__ == "__main__":
    main()
