import os
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

load_dotenv()

# Авторизація
ODATA_URL = os.getenv("ODATA_URL").rstrip("/")
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

ODATA_ENTITY = "Document_Анализы"
MYSQL_TABLE = "et_Document_Анализы"

# Підключення до БД
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)
cursor = conn.cursor(dictionary=True)

# Отримуємо список полів з БД (без created_at, updated_at)
cursor.execute(f"SHOW COLUMNS FROM {MYSQL_TABLE}")
columns = [row['Field'] for row in cursor.fetchall() if row['Field'] not in ('created_at', 'updated_at')]
odata_select = ",".join(columns)

# Визначаємо дату початку
cursor.execute(f"SELECT MAX(`Date`) AS last_date FROM {MYSQL_TABLE}")
row = cursor.fetchone()
if row and row["last_date"]:
    start_date = (row["last_date"] - timedelta(days=15)).strftime("%Y-%m-%dT00:00:00")
else:
    start_date = "2024-08-01T00:00:00"

print(f"📅 Отримуємо дані з OData з дати {start_date}")

skip = 0
inserted = updated = skipped = 0
total_fetched = 0

while True:
    url = (
        f"{ODATA_URL}/{ODATA_ENTITY}"
        f"?$format=json&$orderby=Date&$top=1000&$skip={skip}"
        f"&$filter=Date ge datetime'{start_date}'"
        f"&$select={odata_select}"
    )
    response = requests.get(url, auth=(ODATA_USER, ODATA_PASSWORD))
    response.raise_for_status()
    data = response.json().get("value", [])

    if not data:
        break

    total_fetched += len(data)

    for record in data:
        ref_key = record["Ref_Key"]
        data_version = record.get("DataVersion")

        cursor.execute(
            f"SELECT DataVersion FROM {MYSQL_TABLE} WHERE Ref_Key = %s",
            (ref_key,)
        )
        existing = cursor.fetchone()

        if existing:
            if existing["DataVersion"] == data_version:
                skipped += 1
                continue
            else:
                set_clause = ", ".join(f"`{col}` = %s" for col in columns if col != "Ref_Key")
                values = [record.get(col) for col in columns if col != "Ref_Key"]
                values.append(ref_key)
                cursor.execute(
                    f"UPDATE {MYSQL_TABLE} SET {set_clause} WHERE Ref_Key = %s",
                    values
                )
                updated += 1
        else:
            placeholders = ", ".join(["%s"] * len(columns))
            col_names = ", ".join(f"`{col}`" for col in columns)
            values = [record.get(col) for col in columns]
            cursor.execute(
                f"INSERT INTO {MYSQL_TABLE} ({col_names}) VALUES ({placeholders})",
                values
            )
            inserted += 1

    conn.commit()
    print(f"✅ Пачка {skip//1000+1}: отримано {len(data)} | 🆕 {inserted}, 🔄 {updated}, ⏭️ {skipped}")
    skip += 1000
    time.sleep(1)

cursor.close()
conn.close()

# Підсумки
print("\n📊 Підсумки:")
print(f"🔢 Отримано всього записів: {total_fetched}")
print(f"🆕 Додано нових: {inserted}")
print(f"🔄 Замінено (оновлено): {updated}")
print(f"⏭️ Пропущено без змін: {skipped}")
print("🏁 Завантаження завершено.")
