"""
Скрипт: zp_sales_salary_loader.py

Функція:
- Формує таблицю з продажів та розрахованими преміями через SQL-CTE (без дублювань Role)
- Підтягує shift_uuid з zp_worktime для унікальної ідентифікації зміни
- Отримує DataFrame
- Чистить таблицю zp_sales_salary
- Завантажує нові дані у zp_sales_salary
- Працює для періоду з 2025-05-01 і пізніше
- Використовує змінні з .env: DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

Інструкції:
1. Налаштуйте змінні .env для доступу до БД.
2. Запустіть скрипт: python zp_sales_salary_loader.py
3. Перевірте дані у таблиці wealth0_analytics.zp_sales_salary
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# === Завантаження .env ===
load_dotenv(r"C:\Users\la\OneDrive\Pet Wealth\Analytics\Python_script\.env")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# === SQLAlchemy engine ===
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}",
    connect_args={'charset': 'utf8mb4'}
)

# === SQL-запит з shift_uuid ===
sql_query = """
WITH our_table AS (
    -- Призначив (Сотрудник)
    SELECT
        pr.Recorder,
        LineNumber,
        Period,
        Recorder_Type,
        Номенклатура_Key,
        nom.Description AS Ном_Description,
        nom.АналитикаПоЗарплате_Key,
        an.Description AS Ан_Description,
        nom.ВидНоменклатуры,
        nom.Вид_Key,
        pr.Сотрудник AS Співробітник,
        sp.Графік,
        'Призначив' AS Role,
        Количество,
        КоличествоОплачено,
        Стоимость,
        СтоимостьБезСкидок,
        СуммаЗатрат,
        (Стоимость - СуммаЗатрат) AS ВаловийПрибуток
    FROM wealth0_analytics.et_AccumulationRegister_Продажи_RecordType AS pr
    LEFT JOIN wealth0_analytics.et_Catalog_Номенклатура AS nom
        ON pr.Номенклатура_Key = nom.Ref_Key
    LEFT JOIN wealth0_analytics.et_Catalog_АналитикаПоЗарплате AS an
        ON nom.АналитикаПоЗарплате_Key = an.Ref_Key
    LEFT JOIN wealth0_analytics.zp_довСпівробітники AS sp
        ON pr.Сотрудник = sp.Ref_Key
    WHERE pr.Active = 1 AND Period >= '2025-05-01'

    UNION ALL

    -- Виконавець (Исполнитель)
    SELECT
        pr.Recorder,
        LineNumber,
        Period,
        Recorder_Type,
        Номенклатура_Key,
        nom.Description AS Ном_Description,
        nom.АналитикаПоЗарплате_Key,
        an.Description AS Ан_Description,
        nom.ВидНоменклатуры,
        nom.Вид_Key,
        pr.Исполнитель AS Співробітник,
        sp.Графік,
        'Виконавець' AS Role,
        Количество,
        КоличествоОплачено,
        Стоимость,
        СтоимостьБезСкидок,
        СуммаЗатрат,
        (Стоимость - СуммаЗатрат) AS ВаловийПрибуток
    FROM wealth0_analytics.et_AccumulationRegister_Продажи_RecordType AS pr
    LEFT JOIN wealth0_analytics.et_Catalog_Номенклатура AS nom
        ON pr.Номенклатура_Key = nom.Ref_Key
    LEFT JOIN wealth0_analytics.et_Catalog_АналитикаПоЗарплате AS an
        ON nom.АналитикаПоЗарплате_Key = an.Ref_Key
    LEFT JOIN wealth0_analytics.zp_довСпівробітники AS sp
        ON pr.Исполнитель = sp.Ref_Key
    WHERE pr.Active = 1 AND Period >= '2025-05-01'
)
SELECT
    o.*,
    w.position,
    w.level,
    w.department,
    w.shift_type,
    w.Rule_ID,
    w.shift_uuid,  -- 🆕 Додаємо shift_uuid з zp_worktime
    CASE
        WHEN o.Role = 'Призначив' THEN pay.Ан_Призначив
        WHEN o.Role = 'Виконавець' THEN pay.Ан_Виконав
        ELSE 0
    END AS ВідсотокПремії,
    ROUND(CASE
        WHEN o.Role = 'Призначив' THEN pay.Ан_Призначив * o.Стоимость
        WHEN o.Role = 'Виконавець' THEN pay.Ан_Виконав * o.Стоимость
        ELSE 0
    END, 2) AS ПреміяСтоимость,
    ROUND(CASE
        WHEN o.Role = 'Призначив' THEN pay.Ан_Призначив * o.СтоимостьБезСкидок
        WHEN o.Role = 'Виконавець' THEN pay.Ан_Виконав * o.СтоимостьБезСкидок
        ELSE 0
    END, 2) AS ПреміяСтоимостьБезСкидок,
    ROUND(CASE
        WHEN o.Role = 'Призначив' THEN pay.Ан_Призначив * o.ВаловийПрибуток
        WHEN o.Role = 'Виконавець' THEN pay.Ан_Виконав * o.ВаловийПрибуток
        ELSE 0
    END, 2) AS ПреміяВаловийПрибуток
FROM our_table o
LEFT JOIN wealth0_analytics.zp_worktime w
    ON o.Графік = w.last_name
    AND o.Period >= w.time_start
    AND o.Period < w.time_end
LEFT JOIN wealth0_analytics.zp_фктУмовиОплати pay
    ON w.Rule_ID = pay.Rule_ID
    AND pay.АнЗП = o.Ан_Description
    AND (pay.ДатаЗакінчення IS NULL OR o.Period < pay.ДатаЗакінчення)
WHERE o.Period >= '2025-05-01'
"""

# === Отримання даних ===
print("🔍 Виконуємо SQL-запит...")
df = pd.read_sql(sql_query, con=engine)
print(f"✅ Отримано {len(df)} рядків")

# === Очищення таблиці zp_sales_salary ===
with engine.connect() as conn:
    print("🗑️ Очищення старих записів...")
    conn.execute(text("TRUNCATE TABLE wealth0_analytics.zp_sales_salary"))

# === Завантаження нових даних ===
print("⬇️ Завантажуємо дані у zp_sales_salary...")
df.to_sql(
    name='zp_sales_salary',
    con=engine,
    if_exists='append',
    index=False
)
print("✅ Дані успішно завантажені!")
