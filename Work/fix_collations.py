import os
import mysql.connector
from dotenv import load_dotenv

# Завантажуємо змінні з .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")
TARGET_COLLATION = "utf8mb4_unicode_ci"

# Підключення до MySQL
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)
cursor = conn.cursor()

# Зміна collation бази (за замовчуванням)
print(f"\n📦 Зміна collation бази `{DB_DATABASE}`...")
cursor.execute(f"""
    ALTER DATABASE `{DB_DATABASE}` 
    CHARACTER SET utf8mb4 
    COLLATE {TARGET_COLLATION}
""")

# Пошук всіх текстових колонок з іншим collation
query = f"""
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = %s
  AND COLLATION_NAME IS NOT NULL
  AND COLLATION_NAME != %s
  AND DATA_TYPE IN ('char', 'varchar', 'text', 'mediumtext', 'longtext', 'tinytext')
"""

cursor.execute(query, (DB_DATABASE, TARGET_COLLATION))
columns = cursor.fetchall()

print(f"\n🔎 Знайдено {len(columns)} колонок для оновлення.\n")

# Оновлення кожного поля
for table_name, column_name, column_type, old_collation in columns:
    alter_sql = f"""
    ALTER TABLE `{table_name}` 
    MODIFY COLUMN `{column_name}` {column_type} 
    CHARACTER SET utf8mb4 
    COLLATE {TARGET_COLLATION}
    """
    print(f"🔧 {table_name}.{column_name}: {old_collation} → {TARGET_COLLATION}")
    try:
        cursor.execute(alter_sql)
    except mysql.connector.Error as err:
        print(f"⚠️ Помилка: {err}")

conn.commit()
cursor.close()
conn.close()

print("\n✅ Готово! Collation у всіх полях змінено на utf8mb4_unicode_ci.")
