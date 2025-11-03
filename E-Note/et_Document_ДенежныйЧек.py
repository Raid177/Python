#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL: OData Document_ДенежныйЧек -> MySQL et_Document_ДенежныйЧек (parallel weekly windows)

.env:
  ODATA_URL, ODATA_USER, ODATA_PASSWORD
  DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
"""

import os, time, random
import datetime as dt
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pymysql
from dotenv import load_dotenv

# ====================== НАЛАШТУВАННЯ ======================
LOAD_MODE = "rewrite_range"   # "rewrite_range" або "upsert_by_dataversion"
BATCH_SIZE = 1000             # OData $top
DAYS_BACK = 45                # старт від MAX(Date) - DAYS_BACK; якщо таблиця порожня — START_IF_EMPTY
START_IF_EMPTY = "2024-07-01"

# паралельність
MAX_WORKERS = 4               # 3-5 оптимально
WINDOW_DAYS = 7               # розмір “вікна” у днях (7 = тижні)
RETRY_MAX = 5                 # ретраї на 429/5xx/таймаути
RETRY_BASE_SLEEP = 0.8        # базова затримка бекофу, сек

# короткий $select: постав список полів або залиш None, щоб тягнути все
SHORT_SELECT = None
# приклад легкого селекту:
# SHORT_SELECT = [
#     "Ref_Key","DataVersion","Date","Number","Posted","Комментарий",
#     "Сумма","СуммаБезнал","СуммаКредит","СуммаПлатежаБонусы","СуммаТорговойУступки",
#     "Организация_Key","Подразделение_Key"
# ]

TABLE  = "et_Document_ДенежныйЧек"
ENTITY = "Document_ДенежныйЧек"   # важливо: кирилиця у шляху збережена

# ==========================================================

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
    # каркас, щоб були ключові поля
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
    # не пишемо службові
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

# ------------------ вікна по даті ------------------

def day_windows(start_dt, end_dt, step_days=7):
    cur = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    while cur < end_dt:
        nxt = cur + dt.timedelta(days=step_days)
        yield (cur, min(nxt, end_dt))
        cur = nxt

# ------------------ HTTP клієнт + ретраї ------------------

def session_build():
    sess = requests.Session()
    sess.headers.update({
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "User-Agent": "PW-ETL/1.0"
    })
    return sess

def http_get_with_retries(sess, url, auth):
    for attempt in range(1, RETRY_MAX + 1):
        try:
            resp = sess.get(url, auth=auth, timeout=120)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Transient {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception as e:
            if attempt >= RETRY_MAX:
                raise
            sleep_s = RETRY_BASE_SLEEP * (2 ** (attempt - 1)) * (1.0 + random.random() * 0.2)
            time.sleep(sleep_s)

# ------------------ завантаження одного вікна ------------------

def fetch_window(sess, base, auth, w_start, w_end, batch_size):
    """
    Тягне одне вікно (напіввідкрито): Date >= w_start AND Date < w_end
    Пагінується $skip усередині цього вікна.
    """
    filter_q = (
        f"Date ge datetime'{w_start:%Y-%m-%dT%H:%M:%S}' "
        f"and Date lt datetime'{w_end:%Y-%m-%dT%H:%M:%S}'"
    )
    orderby = "Date,Ref_Key"
    select_clause = f"&$select={','.join(SHORT_SELECT)}" if SHORT_SELECT else ""

    rows_all, skip, batch_idx = [], 0, 1
    while True:
        url = (f"{base}?$format=json&$orderby={orderby}"
               f"&$filter={filter_q}{select_clause}&$top={batch_size}&$skip={skip}")
        resp = http_get_with_retries(sess, url, auth)
        payload = resp.json()
        rows = payload.get("value", [])
        if not rows:
            if batch_idx == 1:
                log(f"[{w_start:%Y-%m-%d}] fetched=0")
            break

        dts = [r.get("Date") for r in rows if r.get("Date")]
        log(f"[{w_start:%Y-%m-%d}..{w_end:%Y-%m-%d}] "
            f"[B{batch_idx}] fetched={len(rows)} | {min(dts) if dts else None} .. {max(dts) if dts else None}")

        rows_all.extend(rows)
        if len(rows) < batch_size:
            break
        skip += batch_size
        batch_idx += 1
    return rows_all

# ------------------ паралельний збір усіх вікон ------------------

def odata_fetch_all_parallel(start_dt):
    base = os.getenv("ODATA_URL").rstrip("/") + f"/{ENTITY}"  # важливо: кирилиця у шляху
    auth = (os.getenv("ODATA_USER"), os.getenv("ODATA_PASSWORD"))
    end_dt = dt.datetime.now().replace(microsecond=0)

    windows = list(day_windows(start_dt, end_dt, step_days=WINDOW_DAYS))
    log(f"Parallel windows: {len(windows)} × {WINDOW_DAYS}-day; workers={MAX_WORKERS}")

    sess = session_build()
    all_rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fut2win = {
            pool.submit(fetch_window, sess, base, auth, w0, w1, BATCH_SIZE): (w0, w1)
            for (w0, w1) in windows
        }
        for fut in as_completed(fut2win):
            w0, w1 = fut2win[fut]
            try:
                part = fut.result()
                all_rows.extend(part)
            except Exception as e:
                log(f"[{w0:%Y-%m-%d}..{w1:%Y-%m-%d}] FAILED: {e}")
                raise

    # впорядкуємо (для стабільності логів / детермінізму)
    try:
        all_rows.sort(key=lambda r: ((r.get("Date") or ""), (r.get("Ref_Key") or "")))
    except Exception:
        pass

    log(f"Total fetched (parallel): {len(all_rows)}")
    return all_rows

# ------------------ нормалізація та запис ------------------

def coerce_value(v):
    # None як є
    if v is None:
        return None
    # bool -> tinyint
    if isinstance(v, bool):
        return int(v)
    # тільки для рядків робимо датоподібну заміну T -> пробіл
    if isinstance(v, str):
        # найпростіша перевірка ISO-дат/часу
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
            out[c] = str(val) if val is not None else None
    return out

def insert_update(conn, rows, cols):
    if not rows: return (0, 0, 0)
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(f"`{c}`" for c in cols)
    updset = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols if c != "Ref_Key")
    sql = f"INSERT INTO `{TABLE}` ({collist}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updset}"
    with conn.cursor() as cur:
        for i in range(0, len(rows), 1000):
            vals = [tuple(r.get(c) for c in cols) for r in rows[i:i+1000]]
            cur.executemany(sql, vals)
    conn.commit()
    return (len(rows), 0, 0)

def upsert_by_dv(conn, rows, cols):
    if not rows: return (0, 0, 0)
    existing = preload_versions(conn)
    to_write, ins, upd, skip = [], 0, 0, 0
    for r in rows:
        ref = r.get("Ref_Key"); dv = r.get("DataVersion")
        if not ref:
            continue
        if ref in existing:
            if existing[ref] == dv:
                skip += 1
                continue
            upd += 1
        else:
            ins += 1
        to_write.append(tuple(r.get(c) for c in cols))
    if to_write:
        placeholders = ", ".join(["%s"] * len(cols))
        collist = ", ".join(f"`{c}`" for c in cols)
        updset = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols if c != "Ref_Key")
        sql = f"INSERT INTO `{TABLE}` ({collist}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updset}"
        with conn.cursor() as cur:
            for i in range(0, len(to_write), 1000):
                cur.executemany(sql, to_write[i:i+1000])
        conn.commit()
    return (ins, upd, skip)

# ------------------ main ------------------

def main():
    load_dotenv()
    conn = mysql_conn()
    try:
        ensure_min_table(conn)
        cols_db = db_columns(conn)

        start_dt = get_start_date(conn)
        log(f"Start ETL: {ENTITY} -> {TABLE}")
        log(f"BATCH_SIZE={BATCH_SIZE}, DAYS_BACK={DAYS_BACK}, WINDOW_DAYS={WINDOW_DAYS}, WORKERS={MAX_WORKERS}, LOAD_MODE={LOAD_MODE}")
        log(f"Start date (filter): {start_dt}")

        if LOAD_MODE == "rewrite_range":
            log("Deleting existing records in range ...")
            delete_range(conn, start_dt)

        raw = odata_fetch_all_parallel(start_dt)
        log(f"Fetched total: {len(raw)}")

        trimmed = [filter_to_db_cols(r, cols_db) for r in raw]

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
