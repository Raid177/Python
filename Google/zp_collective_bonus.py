"""
[START] Скрипт формування таблиці zp_collective_bonus [START]

Функціонал:
- Для кожної зміни з таблиці zp_worktime:
    • Якщо shift_uuid ще не згенеровано, створює його на основі основних полів зміни (date_shift, idx, time_start, time_end, position, department, shift_type).
    • Для кожного правила з таблиці zp_фктУмовиОплати (якщо АнЗП_Колективний заповнено) обчислює суму продажів з таблиці zp_sales_salary за період зміни (тільки Role = 'Призначив').
    • Рахує премії для кожного типу (Стоимость, СтоимостьБезСкидок, ВаловийПрибуток).
    • Записує дані у таблицю zp_collective_bonus (insert або update).
- Логи:
    • Наявність продажів або їх відсутність.
    • Істинні колізії shift_uuid у ворктаймі.
"""

import pymysql
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv

# [START] Завантаження .env
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

# [START] Підключення до БД
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

connection = pymysql.connect(**DB_CONFIG)
cursor = connection.cursor()

print("[SYNC] Розпочато обробку змін...")

cursor.execute("""SELECT * FROM zp_worktime""")
worktime_rows = cursor.fetchall()
print(f"[OK] Отримано {len(worktime_rows)} змін для обробки.")

total_bonuses = 0
true_collisions_detected = 0

for row in worktime_rows:
    shift_uuid = row['shift_uuid']
    if not shift_uuid:
        shift_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{row['date_shift']}-{row['idx']}-{row['time_start']}-{row['time_end']}-{row['position']}-{row['department']}-{row['shift_type']}"))
        cursor.execute("""
            UPDATE zp_worktime
            SET shift_uuid = %s
            WHERE date_shift = %s AND idx = %s
        """, (shift_uuid, row['date_shift'], row['idx']))
        connection.commit()
        print(f"[NEW] Згенеровано shift_uuid для зміни {row['date_shift']} idx={row['idx']}")

    # [INFO] Перевірка на істинні колізії
    cursor.execute("""
        SELECT COUNT(*) AS cnt
        FROM zp_worktime
        WHERE shift_uuid = %s
    """, (shift_uuid,))
    count_row = cursor.fetchone()
    if count_row['cnt'] > 1:
        true_collisions_detected += 1
        print(f"[WARN] Істинна колізія: дубльований shift_uuid={shift_uuid} у таблиці zp_worktime (знайдено {count_row['cnt']} записів)")

    cursor.execute("""
        SELECT *
        FROM zp_фктУмовиОплати
        WHERE Rule_ID = %s AND АнЗП_Колективний IS NOT NULL AND АнЗП_Колективний != ''
    """, (row['Rule_ID'],))
    rule_rows = cursor.fetchall()

    for rule in rule_rows:
        ан_колективний = rule['АнЗП_Колективний']
        відсоток = rule['Ан_Колективний']

        # [INFO] Додаємо фільтр Role = 'Призначив'
        cursor.execute("""
            SELECT 
                SUM(Стоимость) AS СуммаСтоимость,
                SUM(СтоимостьБезСкидок) AS СуммаСтоимостьБезСкидок,
                SUM(ВаловийПрибуток) AS СуммаВаловийПрибуток
            FROM zp_sales_salary
            WHERE Period >= %s AND Period < %s
              AND Ан_Description = %s
              AND Role = 'Призначив'
        """, (row['time_start'], row['time_end'], ан_колективний))
        sales = cursor.fetchone()

        if sales['СуммаСтоимость'] is None:
            print(f"[WARN] Немає продажів для правила {ан_колективний} у зміні {row['date_shift']} idx={row['idx']}")
            continue

        СтоимостьПремія = sales['СуммаСтоимость'] * відсоток
        СтоимостьБезСкидокПремія = sales['СуммаСтоимостьБезСкидок'] * відсоток
        ВаловийПрибутокПремія = sales['СуммаВаловийПрибуток'] * відсоток

        cursor.execute("""
            INSERT INTO zp_collective_bonus (
                shift_uuid, date_shift, last_name, idx, time_start, time_end, position, department, shift_type,
                Rule_ID, АнЗП_Колективний, Відсоток,
                СуммаСтоимость, СуммаСтоимостьБезСкидок, СуммаВаловийПрибуток,
                СтоимостьПремія, СтоимостьБезСкидокПремія, ВаловийПрибутокПремія
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                last_name = VALUES(last_name),
                Відсоток = VALUES(Відсоток),
                СуммаСтоимость = VALUES(СуммаСтоимость),
                СуммаСтоимостьБезСкидок = VALUES(СуммаСтоимостьБезСкидок),
                СуммаВаловийПрибуток = VALUES(СуммаВаловийПрибуток),
                СтоимостьПремія = VALUES(СтоимостьПремія),
                СтоимостьБезСкидокПремія = VALUES(СтоимостьБезСкидокПремія),
                ВаловийПрибутокПремія = VALUES(ВаловийПрибутокПремія),
                updated_at = NOW()
        """, (
            shift_uuid, row['date_shift'], row['last_name'], row['idx'], row['time_start'], row['time_end'],
            row['position'], row['department'], row['shift_type'],
            row['Rule_ID'], ан_колективний, відсоток,
            sales['СуммаСтоимость'], sales['СуммаСтоимостьБезСкидок'], sales['СуммаВаловийПрибуток'],
            СтоимостьПремія, СтоимостьБезСкидокПремія, ВаловийПрибутокПремія
        ))
        connection.commit()

        total_bonuses += 1
        # print(f"[OK] Додано/оновлено премію для shift_uuid={shift_uuid} + {ан_колективний}")

print(f"[FINISH] Обробка завершена: {total_bonuses} записів премій.")
if true_collisions_detected > 0:
    print(f"[WARN] Увага! Виявлено {true_collisions_detected} істинних колізій!")

cursor.close()
connection.close()
