import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)
cursor = conn.cursor()

print(f"\n📦 Аналіз індексів у базі `{DB_DATABASE}`...\n")

# 1. Таблиці без PRIMARY KEY
print("🚫 Таблиці без PRIMARY KEY:")
cursor.execute("""
SELECT t.TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_SCHEMA = %s
AND t.TABLE_TYPE = 'BASE TABLE'
AND NOT EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
    WHERE c.CONSTRAINT_TYPE = 'PRIMARY KEY'
    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND c.TABLE_NAME = t.TABLE_NAME
)
""", (DB_DATABASE,))
for (table_name,) in cursor.fetchall():
    print(f"  - {table_name}")

# 2. Таблиці без жодного індексу
print("\n🕳️ Таблиці без індексів:")
cursor.execute("""
SELECT t.TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_SCHEMA = %s
AND t.TABLE_TYPE = 'BASE TABLE'
AND NOT EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.STATISTICS s
    WHERE s.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND s.TABLE_NAME = t.TABLE_NAME
)
""", (DB_DATABASE,))
for (table_name,) in cursor.fetchall():
    print(f"  - {table_name}")

# 3. Пропозиції для індексів за назвою поля
print("\n🔍 Пропозиції для створення індексів (за назвою поля):")
cursor.execute("""
SELECT TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = %s
AND COLUMN_NAME REGEXP '(_id|Ref_Key|foreign_key|код|edrpou|guid)'
AND COLUMN_KEY = ''
""", (DB_DATABASE,))
for table_name, column_name in cursor.fetchall():
    print(f"  👉 Можна створити індекс: `{table_name}`.`{column_name}`")

cursor.close()
conn.close()

print("\n✅ Аналіз завершено.\n")
