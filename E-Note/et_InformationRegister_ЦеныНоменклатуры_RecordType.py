#!/usr/bin/env python3
# /root/Python/E-Note/et_InformationRegister_ЦеныНоменклатуры_RecordType.py

import os
import time
import requests
import mysql.connector
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlencode, quote

# ===== Параметри (без CLI) =====
BATCH_SIZE = 1000
SLEEP_SECONDS = 1
REQ_TIMEOUT = 60
DEFAULT_START_DATE = datetime(2024, 8, 1, tzinfo=timezone.utc)
DAYS_BACK = 14

# ===== .env із директорії скрипта =====
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ===== OData =====
ODATA_URL = os.getenv("ODATA_URL").rstrip("/") + "/InformationRegister_ЦеныНоменклатуры_RecordType"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

# ===== БД =====
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "autocommit": True,
}

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()
session = requests.Session()
session.auth = (ODATA_USER, ODATA_PASSWORD)
session.headers.update({"Accept": "application/json"})

# ===== Орієнтир по даті з БД =====
cursor.execute("SELECT MAX(Period) FROM et_InformationRegister_ЦеныНоменклатуры_RecordType")
row = cursor.fetchone()
last_date = row[0] if row and row[0] else DEFAULT_START_DATE

now_utc = datetime.now(timezone.utc)
if not isinstance(last_date, datetime):
    last_date = DEFAULT_START_DATE
elif last_date.tzinfo is None:
    last_date = last_date.replace(tzinfo=timezone.utc)
if last_date > now_utc:
    last_date = now_utc

cutoff_dt = max(DEFAULT_START_DATE, last_date - timedelta(days=DAYS_BACK))
odata_cutoff = cutoff_dt.date().strftime("%Y-%m-%dT00:00:00")

print(f"MAX(Period) у БД: {last_date.isoformat()}")
print(f"Завантажуватимемо з: {cutoff_dt.isoformat()}")

# Поля рівно за $metadata
SELECT_FIELDS = (
    "Period,Recorder,Recorder_Type,LineNumber,Active,"
    "ТипЦен_Key,Номенклатура_Key,ЕдиницаИзмерения_Key,Валюта_Key,Цена"
)

def fetch_page(skip: int):
    """
    Основний запит: $filter + $orderby=Period asc + $select.
    Пробіли кодуємо як %20 (urlencode(..., quote_via=quote)), щоб 1С не ламалась.
    Фолбеки на 500: без $select, потім без $orderby.
    """
    base = {
        "$format": "json",
        "$top": BATCH_SIZE,
        "$skip": skip,
        "$orderby": "Period asc",
        "$filter": f"Period ge datetime'{odata_cutoff}'",
        "$select": SELECT_FIELDS,
    }
    qs = urlencode(base, safe="'", quote_via=quote)
    url = f"{ODATA_URL}?{qs}"

    r = session.get(url, timeout=REQ_TIMEOUT)
    if r.status_code == 500:
        # fallback 1: без $select
        base2 = dict(base); base2.pop("$select", None)
        qs2 = urlencode(base2, safe="'", quote_via=quote)
        r = session.get(f"{ODATA_URL}?{qs2}", timeout=REQ_TIMEOUT)
        if r.status_code == 500:
            # fallback 2: тільки фільтр (без orderby/select)
            base3 = {
                "$format": "json",
                "$top": BATCH_SIZE,
                "$skip": skip,
                "$filter": f"Period ge datetime'{odata_cutoff}'",
            }
            qs3 = urlencode(base3, safe="'", quote_via=quote)
            r = session.get(f"{ODATA_URL}?{qs3}", timeout=REQ_TIMEOUT)

    if r.status_code >= 400:
        print("ODATA ERROR:", r.status_code, r.text[:800])
    r.raise_for_status()
    return r.json().get("value", [])

def upsert_rows(items):
    """INSERT ... ON DUPLICATE KEY UPDATE по композитному ключу таблиці."""
    global written_total
    if not items:
        return
    conn.ping(reconnect=True, attempts=2, delay=1)
    for row in items:
        cursor.execute(
            """
            INSERT INTO et_InformationRegister_ЦеныНоменклатуры_RecordType (
                Period, Recorder, Recorder_Type, LineNumber, Active,
                ТипЦен_Key, Номенклатура_Key, ЕдиницаИзмерения_Key, Валюта_Key, Цена,
                created_at, updated_at
            ) VALUES (
                %(Period)s, %(Recorder)s, %(Recorder_Type)s, %(LineNumber)s, %(Active)s,
                %(ТипЦен_Key)s, %(Номенклатура_Key)s, %(ЕдиницаИзмерения_Key)s, %(Валюта_Key)s, %(Цена)s,
                NOW(), NOW()
            )
            ON DUPLICATE KEY UPDATE
                Period = VALUES(Period),
                Recorder = VALUES(Recorder),
                Recorder_Type = VALUES(Recorder_Type),
                LineNumber = VALUES(LineNumber),
                Active = VALUES(Active),
                ТипЦен_Key = VALUES(ТипЦен_Key),
                Номенклатура_Key = VALUES(Номенклатура_Key),
                ЕдиницаИзмерения_Key = VALUES(ЕдиницаИзмерения_Key),
                Валюта_Key = VALUES(Валюта_Key),
                Цена = VALUES(Цена),
                updated_at = NOW()
            """,
            {
                "Period": row.get("Period"),
                "Recorder": row.get("Recorder"),
                "Recorder_Type": row.get("Recorder_Type"),
                "LineNumber": row.get("LineNumber"),   # Int64-safe, без касту
                "Active": row.get("Active"),
                "ТипЦен_Key": row.get("ТипЦен_Key"),
                "Номенклатура_Key": row.get("Номенклатура_Key"),
                "ЕдиницаИзмерения_Key": row.get("ЕдиницаИзмерения_Key"),
                "Валюта_Key": row.get("Валюта_Key"),
                "Цена": row.get("Цена"),
            },
        )
        written_total += 1

if __name__ == "__main__":
    received_total = 0
    written_total = 0
    skip = 0

    while True:
        page = fetch_page(skip)
        if not page:
            break
        print(f"Отримано з OData: {len(page)}")
        upsert_rows(page)
        received_total += len(page)
        skip += BATCH_SIZE
        time.sleep(SLEEP_SECONDS)

    print("\n🔚 Готово.")
    print(f"Усього отримано: {received_total}")
    print(f"Записано/оновлено: {written_total}")

    cursor.close()
    conn.close()
