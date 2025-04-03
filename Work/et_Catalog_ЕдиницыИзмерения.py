import os
import time
import requests
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# Авторизація
ODATA_URL = os.getenv("ODATA_URL") + "Catalog_ЕдиницыИзмерения"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "autocommit": True
}

BATCH_SIZE = 1000

# Підключення до БД
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)

def fetch_existing_refs():
    cursor.execute("SELECT Ref_Key, DataVersion FROM et_Catalog_ЕдиницыИзмерения")
    return {row["Ref_Key"]: row["DataVersion"] for row in cursor.fetchall()}

def upsert_entry(entry, existing_refs, updated_count, inserted_count):
    ref_key = entry["Ref_Key"]
    data_version = entry["DataVersion"]

    if ref_key in existing_refs:
        if existing_refs[ref_key] != data_version:
            # оновлюємо
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
                entry["DeletionMark"],
                entry["Owner_Key"],
                entry["Code"],
                entry["Description"],
                entry["Коэффициент"],
                entry["Вес"],
                entry["ID"],
                entry["ВидУпаковкиМаркированногоПродукта"],
                entry["Predefined"],
                entry["PredefinedDataName"],
                ref_key
            ))
            updated_count[0] += 1
    else:
        # вставка
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
            entry["DeletionMark"],
            entry["Owner_Key"],
            entry["Code"],
            entry["Description"],
            entry["Коэффициент"],
            entry["Вес"],
            entry["ID"],
            entry["ВидУпаковкиМаркированногоПродукта"],
            entry["Predefined"],
            entry["PredefinedDataName"]
        ))
        inserted_count[0] += 1

def fetch_odata_batch(skip):
    params = {
        "$orderby": "Ref_Key",
        "$top": BATCH_SIZE,
        "$skip": skip,
        "$format": "json"
    }
    response = requests.get(ODATA_URL, auth=(ODATA_USER, ODATA_PASSWORD), params=params)

    try:
        response.raise_for_status()
        return response.json().get("value", [])
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print("Response text:", response.text)
    except requests.exceptions.JSONDecodeError as json_err:
        print("JSON decode error:", json_err)
        print("Response text:", response.text)
    
    return []


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

        for entry in batch:
            upsert_entry(entry, existing_refs, updated_count, inserted_count)

        total_fetched += len(batch)
        skip += BATCH_SIZE
        time.sleep(1)

    print(f"Отримано записів з ОДата: {total_fetched}")
    print(f"Змінено записів: {updated_count[0]}")
    print(f"Вставлено нових записів: {inserted_count[0]}")

if __name__ == "__main__":
    main()
