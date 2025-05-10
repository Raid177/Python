import os
import requests
import mysql.connector
from dotenv import load_dotenv
from urllib.parse import urlencode

# Завантаження змінних з .env
load_dotenv('C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env')

ODATA_URL = os.getenv("ODATA_URL")
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

TABLE_NAME = "et_InformationRegister_Штрихкоды"
ODATA_ENTITY = "InformationRegister_Штрихкоды"
BATCH_SIZE = 5000

# Отримуємо список полів із БД (без created_at, updated_at)
def get_table_fields(cursor):
    cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
          AND COLUMN_NAME NOT IN ('created_at', 'updated_at')
    """, (DB_DATABASE, TABLE_NAME))
    return [row[0] for row in cursor.fetchall()]

# Створюємо рядок SELECT для OData
def build_select_clause(fields):
    return ','.join(fields)

# Головна функція
def main():
    # Підключення до БД
    conn = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE
    )
    cursor = conn.cursor()

    fields = get_table_fields(cursor)
    select_clause = build_select_clause(fields)

    skip = 0
    total_inserted = 0
    total_updated = 0

    while True:
        params = {
            "$format": "json",
            "$top": BATCH_SIZE,
            "$skip": skip,
            "$select": select_clause
        }
        url = f"{ODATA_URL}{ODATA_ENTITY}?{urlencode(params)}"
        response = requests.get(url, auth=(ODATA_USER, ODATA_PASSWORD))

        if response.status_code != 200:
            print(f"❌ Запит помилковий: {response.status_code} → {response.text}")
            break

        data = response.json().get("value", [])
        if not data:
            break  # Кінець

        for row in data:
            placeholders = ', '.join(['%s'] * len(fields))
            update_clause = ', '.join([f"{f}=VALUES({f})" for f in fields if f not in ('Штрихкод', 'Номенклатура')])
            sql = f"""
                INSERT INTO {TABLE_NAME} ({','.join(fields)})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
            cursor.execute(sql, [row.get(f) for f in fields])

            if cursor.rowcount == 1:
                total_inserted += 1
            elif cursor.rowcount == 2:
                total_updated += 1

        conn.commit()
        print(f"✅ Опрацьовано {len(data)} записів (всього: {skip + len(data)})")
        skip += BATCH_SIZE

    print(f"\n🏁 Завершено: додано {total_inserted}, оновлено {total_updated}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
