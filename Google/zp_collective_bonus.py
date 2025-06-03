"""
🚀 Скрипт формування таблиці zp_collective_bonus 🚀

Функціонал:
- Для кожної зміни з таблиці zp_worktime:
    • Якщо shift_uuid ще не згенеровано, створює його на основі основних полів зміни (date_shift, idx, time_start, time_end, position, department, shift_type).
    • Для кожного правила з таблиці zp_фктУмовиОплати (якщо АнЗП_Колективний заповнено) обчислює суму продажів з таблиці zp_sales_salary за період зміни.
    • Обчислює премії для кожного типу (Стоимость, СтоимостьБезСкидок, ВаловийПрибуток).
    • Записує дані у таблицю zp_collective_bonus (insert або update).
- Забезпечує єдиний зв’язок між таблицями через shift_uuid.

✅ Скрипт можна запускати регулярно для актуалізації колективних премій.
"""


import pymysql
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv

# Завантажуємо .env
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

# Підключення до бази
connection = pymysql.connect(**DB_CONFIG)
cursor = connection.cursor()

# 1. Отримати дані з zp_worktime
cursor.execute("""
    SELECT * FROM zp_worktime
""")
worktime_rows = cursor.fetchall()

for row in worktime_rows:
    shift_uuid = row['shift_uuid']
    if not shift_uuid:
        # Якщо shift_uuid відсутній, генеруємо
        shift_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{row['date_shift']}-{row['idx']}-{row['time_start']}-{row['time_end']}-{row['position']}-{row['department']}-{row['shift_type']}"))
        cursor.execute("""
            UPDATE zp_worktime
            SET shift_uuid = %s
            WHERE date_shift = %s AND idx = %s
        """, (shift_uuid, row['date_shift'], row['idx']))
        connection.commit()

    # 2. Отримати всі правила для Rule_ID
    cursor.execute("""
        SELECT *
        FROM zp_фктУмовиОплати
        WHERE Rule_ID = %s AND АнЗП_Колективний IS NOT NULL AND АнЗП_Колективний != ''
    """, (row['Rule_ID'],))
    rule_rows = cursor.fetchall()

    for rule in rule_rows:
        ан_колективний = rule['АнЗП_Колективний']
        відсоток = rule['Ан_Колективний']

        # 3. Отримати суму продажів за зміну (zp_sales_salary)
        cursor.execute("""
            SELECT 
                SUM(Стоимость) AS СуммаСтоимость,
                SUM(СтоимостьБезСкидок) AS СуммаСтоимостьБезСкидок,
                SUM(ВаловийПрибуток) AS СуммаВаловийПрибуток
            FROM zp_sales_salary
            WHERE Period >= %s AND Period < %s
              AND position = %s
              AND department = %s
              AND shift_type = %s
              AND Rule_ID = %s
              AND Ан_Description = %s
        """, (row['time_start'], row['time_end'], row['position'], row['department'], row['shift_type'], row['Rule_ID'], ан_колективний))
        sales = cursor.fetchone()

        if sales['СуммаСтоимость'] is None:
            continue  # Якщо немає продажів

        # 4. Обчислити премії
        СтоимостьПремія = sales['СуммаСтоимость'] * відсоток
        СтоимостьБезСкидокПремія = sales['СуммаСтоимостьБезСкидок'] * відсоток
        ВаловийПрибутокПремія = sales['СуммаВаловийПрибуток'] * відсоток

        # 5. INSERT/UPDATE у zp_collective_bonus
        cursor.execute("""
            INSERT INTO zp_collective_bonus (
                shift_uuid, date_shift, idx, time_start, time_end, position, department, shift_type,
                Rule_ID, АнЗП_Колективний, Відсоток,
                СуммаСтоимость, СуммаСтоимостьБезСкидок, СуммаВаловийПрибуток,
                СтоимостьПремія, СтоимостьБезСкидокПремія, ВаловийПрибутокПремія
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                Відсоток = VALUES(Відсоток),
                СуммаСтоимость = VALUES(СуммаСтоимость),
                СуммаСтоимостьБезСкидок = VALUES(СуммаСтоимостьБезСкидок),
                СуммаВаловийПрибуток = VALUES(СуммаВаловийПрибуток),
                СтоимостьПремія = VALUES(СтоимостьПремія),
                СтоимостьБезСкидокПремія = VALUES(СтоимостьБезСкидокПремія),
                ВаловийПрибутокПремія = VALUES(ВаловийПрибутокПремія),
                updated_at = NOW()
        """, (
            shift_uuid, row['date_shift'], row['idx'], row['time_start'], row['time_end'],
            row['position'], row['department'], row['shift_type'], row['Rule_ID'], ан_колективний, відсоток,
            sales['СуммаСтоимость'], sales['СуммаСтоимостьБезСкидок'], sales['СуммаВаловийПрибуток'],
            СтоимостьПремія, СтоимостьБезСкидокПремія, ВаловийПрибутокПремія
        ))
        connection.commit()
