#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Union ETL: et_Document_ДенежныйЧек + et_Document_РозничныйЧек -> et_x_Чеки

- Вікно інкремента: [max(Date) у цілі - DAYS_BACK; now()], якщо таблиця порожня — з DEFAULT_START.
- RUN_CHILD_UPDATES=true: перед union виконує et_Document_ДенежныйЧек.py та et_Document_РозничныйЧек.py з цієї ж папки.
- LOAD_MODE: rewrite_range | upsert
- DataVersion: оновлення лише якщо змінилась.
"""

import os
import sys
import time
import datetime as dt
import subprocess

import pymysql
from dotenv import load_dotenv

# ------------------ CONFIG ------------------
DEFAULT_START = dt.datetime(2024, 7, 1, 0, 0, 0)

load_dotenv()
DAYS_BACK = int(os.getenv("DAYS_BACK", "15"))
LOAD_MODE = os.getenv("LOAD_MODE", "rewrite_range").strip().lower()  # rewrite_range | upsert
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.0"))
RUN_CHILD_UPDATES = os.getenv("RUN_CHILD_UPDATES", "true").strip().lower() == "true"

TARGET_TABLE = "et_x_Чеки"
SRC_CASH = "et_Document_ДенежныйЧек"
SRC_RETAIL = "et_Document_РозничныйЧек"

# ------------------ DB ENV ------------------
DB_HOST = os.getenv("DB_HOST") or os.getenv("DB_HOST_Serv", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", os.getenv("DB_PORT_Serv", "3306")))
DB_USER = os.getenv("DB_USER") or os.getenv("DB_USER_Serv")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("DB_PASSWORD_Serv")
DB_DATABASE = os.getenv("DB_DATABASE") or os.getenv("DB_DATABASE_Serv")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def db():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_DATABASE, charset="utf8mb4", autocommit=False,
        cursorclass=pymysql.cursors.DictCursor
    )

def run_child(script_name: str) -> bool:
    path = os.path.join(BASE_DIR, script_name)
    if not os.path.isfile(path):
        print(f"[WARN] Child script not found: {script_name}")
        return True
    print(f"[INFO] Running child: {script_name}")
    proc = subprocess.run([sys.executable, path], cwd=BASE_DIR)
    if proc.returncode != 0:
        print(f"[ERROR] Child failed: {script_name} (exit={proc.returncode})")
        return False
    return True

def get_target_max_date(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(`Date`) AS mx FROM {TARGET_TABLE}")
        row = cur.fetchone()
        return row["mx"]

def get_window():
    conn = db()
    try:
        mx = get_target_max_date(conn)
    finally:
        conn.close()
    if not mx:
        start = DEFAULT_START
    else:
        start = mx - dt.timedelta(days=DAYS_BACK)
    end = dt.datetime.now()
    print(f"[INFO] Window: {start} .. {end}")
    return start, end

def paged_fetch(conn, sql, params, batch_size=BATCH_SIZE, sleep=SLEEP_SECONDS):
    off = 0
    total = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(sql, (*params, batch_size, off))
            rows = cur.fetchall()
        if not rows:
            break
        yield rows
        cnt = len(rows)
        total += cnt
        off += batch_size
        if sleep:
            time.sleep(sleep)
    # print total outside if needed

def fetch_cash(conn, start, end):
    sql = f"""
        SELECT
            Ref_Key, DataVersion, DeletionMark, Number, Date, Posted,
            ВидДвижения,
            ДенежныйСчет,
            ДенежныйСчетБезнал_Key,
            НаправлениеДвижения,
            Организация_Key, Ответственный_Key, Подразделение_Key,
            Сумма, СуммаБезнал,
            ФискальныйНомерЧека
        FROM {SRC_CASH}
        WHERE Date >= %s AND Date <= %s
        ORDER BY Date, Ref_Key
        LIMIT %s OFFSET %s
    """
    res = []
    total = 0
    for chunk in paged_fetch(conn, sql, (start, end)):
        for r in chunk:
            r["Джерело"] = "ДенежныйЧек"
        res.extend(chunk)
        total += len(chunk)
    print(f"[INFO] Cash fetched: {total}")
    return res

def map_vid_oper_to_napr(v):
    if v == "Продажа":
        return "Приход"
    if v == "Возврат":
        return "Расход"
    return v  # інші значення — залишаємо як є

def fetch_retail(conn, start, end):
    sql = f"""
        SELECT
            Ref_Key, DataVersion, DeletionMark, Number, Date, Posted,
            ДенежныйСчет_Key,
            ДенежныйСчетБезнал_Key,
            ВидОперации,
            Организация_Key, Ответственный_Key, Подразделение_Key,
            СуммаОплатыНал, СуммаОплатыБезнал,
            ФискальныйНомерЧека
        FROM {SRC_RETAIL}
        WHERE Date >= %s AND Date <= %s
        ORDER BY Date, Ref_Key
        LIMIT %s OFFSET %s
    """
    res = []
    total = 0
    for chunk in paged_fetch(conn, sql, (start, end)):
        mapped = []
        for r in chunk:
            mapped.append({
                "Ref_Key": r["Ref_Key"],
                "DataVersion": r["DataVersion"],
                "DeletionMark": r["DeletionMark"],
                "Number": r["Number"],
                "Date": r["Date"],
                "Posted": r["Posted"],
                "ВидДвижения": "Роздріб",
                "ДенежныйСчет": r["ДенежныйСчет_Key"],
                "ДенежныйСчетБезнал_Key": r["ДенежныйСчетБезнал_Key"],
                "НаправлениеДвижения": map_vid_oper_to_napr(r["ВидОперации"]),
                "Организация_Key": r["Организация_Key"],
                "Ответственный_Key": r["Ответственный_Key"],
                "Подразделение_Key": r["Подразделение_Key"],
                "Сумма": r["СуммаОплатыНал"],
                "СуммаБезнал": r["СуммаОплатыБезнал"],
                "ФискальныйНомерЧека": r["ФискальныйНомерЧека"],
                "Джерело": "РозничныйЧек",
            })
        res.extend(mapped)
        total += len(mapped)
    print(f"[INFO] Retail fetched: {total}")
    return res

def delete_range(conn, start, end):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {TARGET_TABLE} WHERE `Date` >= %s AND `Date` <= %s", (start, end))
        print(f"[INFO] Deleted in window: {cur.rowcount}")

def bulk_upsert(conn, rows):
    """
    ON DUPLICATE KEY UPDATE: оновлюємо бізнес-поля лише коли змінилась DataVersion.
    updated_at змінюємо тільки при зміні DataVersion.
    """
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {TARGET_TABLE} (
          Ref_Key,DataVersion,DeletionMark,Number,Date,Posted,
          ВидДвижения,ДенежныйСчет,ДенежныйСчетБезнал_Key,НаправлениеДвижения,
          Организация_Key,Ответственный_Key,Подразделение_Key,
          Сумма,СуммаБезнал,ФискальныйНомерЧека,
          Джерело,created_at,updated_at
        ) VALUES (
          %(Ref_Key)s,%(DataVersion)s,%(DeletionMark)s,%(Number)s,%(Date)s,%(Posted)s,
          %(ВидДвижения)s,%(ДенежныйСчет)s,%(ДенежныйСчетБезнал_Key)s,%(НаправлениеДвижения)s,
          %(Организация_Key)s,%(Ответственный_Key)s,%(Подразделение_Key)s,
          %(Сумма)s,%(СуммаБезнал)s,%(ФискальныйНомерЧека)s,
          %(Джерело)s,NOW(),NOW()
        )
        ON DUPLICATE KEY UPDATE
          DataVersion = IF(VALUES(DataVersion)<>DataVersion, VALUES(DataVersion), DataVersion),
          DeletionMark = IF(VALUES(DataVersion)<>DataVersion, VALUES(DeletionMark), DeletionMark),
          Number = IF(VALUES(DataVersion)<>DataVersion, VALUES(Number), Number),
          Date = IF(VALUES(DataVersion)<>DataVersion, VALUES(Date), Date),
          Posted = IF(VALUES(DataVersion)<>DataVersion, VALUES(Posted), Posted),
          ВидДвижения = IF(VALUES(DataVersion)<>DataVersion, VALUES(ВидДвижения), ВидДвижения),
          ДенежныйСчет = IF(VALUES(DataVersion)<>DataVersion, VALUES(ДенежныйСчет), ДенежныйСчет),
          ДенежныйСчетБезнал_Key = IF(VALUES(DataVersion)<>DataVersion, VALUES(ДенежныйСчетБезнал_Key), ДенежныйСчетБезнал_Key),
          НаправлениеДвижения = IF(VALUES(DataVersion)<>DataVersion, VALUES(НаправлениеДвижения), НаправлениеДвижения),
          Организация_Key = IF(VALUES(DataVersion)<>DataVersion, VALUES(Организация_Key), Организация_Key),
          Ответственный_Key = IF(VALUES(DataVersion)<>DataVersion, VALUES(Ответственный_Key), Ответственный_Key),
          Подразделение_Key = IF(VALUES(DataVersion)<>DataVersion, VALUES(Подразделение_Key), Подразделение_Key),
          Сумма = IF(VALUES(DataVersion)<>DataVersion, VALUES(Сумма), Сумма),
          СуммаБезнал = IF(VALUES(DataVersion)<>DataVersion, VALUES(СуммаБезнал), СуммаБезнал),
          ФискальныйНомерЧека = IF(VALUES(DataVersion)<>DataVersion, VALUES(ФискальныйНомерЧека), ФискальныйНомерЧека),
          Джерело = IF(VALUES(DataVersion)<>DataVersion, VALUES(Джерело), Джерело),
          updated_at = IF(VALUES(DataVersion)<>DataVersion, NOW(), updated_at)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        return cur.rowcount

def run():
    print(f"[START] Union ETL -> {TARGET_TABLE}")
    if RUN_CHILD_UPDATES:
        ok1 = run_child("et_Document_ДенежныйЧек.py")
        ok2 = run_child("et_Document_РозничныйЧек.py")
        if not (ok1 and ok2):
            print("[ERROR] Child updates failed — aborting union.")
            sys.exit(1)

    start, end = get_window()
    conn = db()

    try:
        cash = fetch_cash(conn, start, end)
        retail = fetch_retail(conn, start, end)
        all_rows = cash + retail
        print(f"[INFO] Total rows to load: {len(all_rows)}")

        conn.begin()
        if LOAD_MODE == "rewrite_range":
            delete_range(conn, start, end)
            affected = bulk_upsert(conn, all_rows)  # перезапишемо "чисто" вікно
            conn.commit()
            print(f"[DONE] rewrite_range: affected={affected}")
        else:
            affected = bulk_upsert(conn, all_rows)
            conn.commit()
            print(f"[DONE] upsert: affected={affected}")

    except Exception as e:
        conn.rollback()
        print("[ERROR]", repr(e))
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run()
