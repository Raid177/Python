import os
import requests
import mysql.connector
from dotenv import load_dotenv
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

ODATA_ENTITY = "Catalog_Карточки"
MYSQL_TABLE = "et_Catalog_Карточки"

# Підключення до БД
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)
cursor = conn.cursor(dictionary=True)

# Отримуємо список полів з таблиці, крім службових
cursor.execute(f"SHOW COLUMNS FROM {MYSQL_TABLE}")
columns = [row['Field'] for row in cursor.fetchall() if row['Field'] not in ('created_at', 'updated_at')]
odata_select = ",".join(columns)

print("📥 Починаємо завантаження даних з OData...")
skip = 0
inserted = updated = skipped = 0
total_fetched = 0

while True:
    url = (
        f"{ODATA_URL}/{ODATA_ENTITY}"
        f"?$format=json&$orderby=Ref_Key&$top=1000&$skip={skip}"
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

        # Перевірка існуючого запису
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

# Підсумок
print("\n📊 Підсумки:")
print(f"🔢 Отримано всього записів: {total_fetched}")
print(f"🆕 Додано нових записів: {inserted}")
print(f"🔄 Замінено (оновлено): {updated}")
print(f"⏭️ Пропущено без змін: {skipped}")
print("🏁 Завантаження завершено.")
