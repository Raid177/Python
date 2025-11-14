# /root/Python/E-Note/et_Catalog_ЕдиницыИзмерения.py
import os
import time
import requests
import mysql.connector
from pathlib import Path
from dotenv import load_dotenv

# === Константи (без CLI) ===
BATCH_SIZE = 1000
SLEEP_SECONDS = 1
REQ_TIMEOUT = 60

# Явно вантажимо .env із папки скрипта
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Авторизація
ODATA_URL = os.getenv("ODATA_URL").rstrip("/") + "/Catalog_ЕдиницыИзмерения"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "autocommit": True,
}

# Підключення до БД
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)

session = requests.Session()
session.auth = (ODATA_USER, ODATA_PASSWORD)

def fetch_existing_refs():
    conn.ping(reconnect=True, attempts=2, delay=1)
    cursor.execute("SELECT Ref_Key, DataVersion FROM et_Catalog_ЕдиницыИзмерения")
    return {row["Ref_Key"]: row["DataVersion"] for row in cursor.fetchall()}

def upsert_entry(entry, existing_refs, updated_count, inserted_count):
    ref_key = entry["Ref_Key"]
    data_version = entry["DataVersion"]

    if ref_key in existing_refs:
        if existing_refs[ref_key] != data_version:
            update_sql = """
            UPDATE et_Catalog_ЕдиницыИзмерения SET
                DataVersion = %s,
                DeletionMark = %s,
                Owner_Key = %s,
                Code = %s,
                Description = %s,
                Коэффициент = %s,
                Вес = %s,
                ID = %s,
                ВидУпаковкиМаркированногоПродукта = %s,
                Predefined = %s,
                PredefinedDataName = %s,
                updated_at = NOW()
            WHERE Ref_Key = %s
            """
            cursor.execute(update_sql, (
                data_version,
                entry.get("DeletionMark"),
                entry.get("Owner_Key"),
                entry.get("Code"),
                entry.get("Description"),
                entry.get("Коэффициент"),
                entry.get("Вес"),
                entry.get("ID"),
                entry.get("ВидУпаковкиМаркированногоПродукта"),
                entry.get("Predefined"),
                entry.get("PredefinedDataName"),
                ref_key
            ))
            updated_count[0] += 1
    else:
        insert_sql = """
        INSERT INTO et_Catalog_ЕдиницыИзмерения (
            Ref_Key, DataVersion, DeletionMark, Owner_Key, Code, Description,
            Коэффициент, Вес, ID, ВидУпаковкиМаркированногоПродукта,
            Predefined, PredefinedDataName, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """
        cursor.execute(insert_sql, (
            ref_key,
            data_version,
            entry.get("DeletionMark"),
            entry.get("Owner_Key"),
            entry.get("Code"),
            entry.get("Description"),
            entry.get("Коэффициент"),
            entry.get("Вес"),
            entry.get("ID"),
            entry.get("ВидУпаковкиМаркированногоПродукта"),
            entry.get("Predefined"),
            entry.get("PredefinedDataName"),
        ))
        inserted_count[0] += 1

def fetch_odata_batch(skip):
    params = {
        "$orderby": "Ref_Key",
        "$top": BATCH_SIZE,
        "$skip": skip,
        "$format": "json",
        "$select": ",".join([
            "Ref_Key","DataVersion","DeletionMark","Owner_Key","Code","Description",
            "Коэффициент","Вес","ID","ВидУпаковкиМаркированногоПродукта","Predefined","PredefinedDataName"
        ])
    }
    r = session.get(ODATA_URL, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.json().get("value", [])

def main():
    skip = 0
    total_fetched = 0
    updated_count = [0]
    inserted_count = [0]

    existing_refs = fetch_existing_refs()

    while True:
        batch = fetch_odata_batch(skip)
        if not batch:
            break

        conn.ping(reconnect=True, attempts=2, delay=1)
        for entry in batch:
            upsert_entry(entry, existing_refs, updated_count, inserted_count)

        total_fetched += len(batch)
        skip += BATCH_SIZE
        time.sleep(SLEEP_SECONDS)

    print(f"Отримано з OData: {total_fetched}")
    print(f"Оновлено: {updated_count[0]}")
    print(f"Додано: {inserted_count[0]}")

if __name__ == "__main__":
    main()
