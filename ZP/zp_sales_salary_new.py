# -*- coding: utf-8 -*-
"""
Скрипт: zp_sales_salary_loader.py

Функція:
- Формує таблицю з продажів та розрахованими преміями через SQL-CTE (без дублювань Role)
- Підтягує shift_uuid з zp_worktime для унікальної ідентифікації зміни
- Отримує DataFrame
- Чистить таблицю zp_sales_salary
- Завантажує нові дані у zp_sales_salary
- Працює для періоду з 2025-05-01 і пізніше
- Використовує змінні з .env: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from urllib.parse import quote_plus

# ========= ENV loader =========
ENV_PROFILE = os.getenv("ENV_PROFILE", "prod")  # dev | prod
ENV_PATHS = {
    "dev":  r"C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env",
    "prod": "/root/Python/_Acces/.env.prod",
}
ENV_PATH = os.getenv("ENV_PATH") or ENV_PATHS.get(ENV_PROFILE)
if ENV_PATH and os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)
else:
    load_dotenv(override=True)  # шукає .env у поточній папці

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"[ENV ERROR] Missing {name}. Check {ENV_PATH or '.env'}.")
    return v

# === DB creds (увага на назви змінних) ===
DB_HOST = require_env("DB_HOST")
DB_PORT = int(require_env("DB_PORT"))
DB_USER = require_env("DB_USER")
DB_PASSWORD = require_env("DB_PASSWORD")
DB_DATABASE = require_env("DB_DATABASE")

# === SQLAlchemy engine ===
DB_PASSWORD_ENC = quote_plus(DB_PASSWORD)
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD_ENC}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}",
    connect_args={'charset': 'utf8mb4'}
)

# Загальна дата старту (узгоджено в усіх фільтрах)
START_DATE = "2025-05-01"

# === SQL-запит з shift_uuid ===
sql_query = f"""
WITH our_table AS (
    -- Призначив (Сотрудник)
    SELECT
        pr.Recorder,
        pr.LineNumber,
        pr.Period,
        pr.Recorder_Type,
        pr.Номенклатура_Key,
        nom.Description AS Ном_Description,
        nom.АналитикаПоЗарплате_Key,
        an.Description AS Ан_Description,
        nom.ВидНоменклатуры,
        nom.Вид_Key,
        pr.Сотрудник AS Співробітник,
        sp.Графік,
        'Призначив' AS Role,
        pr.Количество,
        pr.КоличествоОплачено,
        pr.Стоимость,
        pr.СтоимостьБезСкидок,
        pr.СуммаЗатрат,
        (pr.Стоимость - pr.СуммаЗатрат) AS ВаловийПрибуток
    FROM petwealth.et_AccumulationRegister_Продажи_RecordType AS pr
    LEFT JOIN petwealth.et_Catalog_Номенклатура AS nom
        ON pr.Номенклатура_Key = nom.Ref_Key
    LEFT JOIN petwealth.et_Catalog_АналитикаПоЗарплате AS an
        ON nom.АналитикаПоЗарплате_Key = an.Ref_Key
    LEFT JOIN petwealth.zp_довСпівробітники AS sp
        ON pr.Сотрудник = sp.Ref_Key
    WHERE pr.Active = 1 AND pr.Period >= '{START_DATE}'

    UNION ALL

    -- Виконавець (Исполнитель)
    SELECT
        pr.Recorder,
        pr.LineNumber,
        pr.Period,
        pr.Recorder_Type,
        pr.Номенклатура_Key,
        nom.Description AS Ном_Description,
        nom.АналитикаПоЗарплате_Key,
        an.Description AS Ан_Description,
        nom.ВидНоменклатуры,
        nom.Вид_Key,
        pr.Исполнитель AS Співробітник,
        sp.Графік,
        'Виконавець' AS Role,
        pr.Количество,
        pr.КоличествоОплачено,
        pr.Стоимость,
        pr.СтоимостьБезСкидок,
        pr.СуммаЗатрат,
        (pr.Стоимость - pr.СуммаЗатрат) AS ВаловийПрибуток
    FROM petwealth.et_AccumulationRegister_Продажи_RecordType AS pr
    LEFT JOIN petwealth.et_Catalog_Номенклатура AS nom
        ON pr.Номенклатура_Key = nom.Ref_Key
    LEFT JOIN petwealth.et_Catalog_АналитикаПоЗарплате AS an
        ON nom.АналитикаПоЗарплате_Key = an.Ref_Key
    LEFT JOIN petwealth.zp_довСпівробітники AS sp
        ON pr.Исполнитель = sp.Ref_Key
    WHERE pr.Active = 1 AND pr.Period >= '{START_DATE}'
)
SELECT
    o.*,
    w.position,
    w.level,
    w.department,
    w.shift_type,
    w.Rule_ID,
    w.shift_uuid,  -- з zp_worktime
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
LEFT JOIN petwealth.zp_worktime w
    ON o.Графік = w.last_name
    AND o.Period >= w.time_start
    AND o.Period <  w.time_end
LEFT JOIN petwealth.zp_фктУмовиОплати pay
    ON w.Rule_ID = pay.Rule_ID
    AND pay.АнЗП = o.Ан_Description
    AND (pay.ДатаПочатку IS NULL OR o.Period >= pay.ДатаПочатку)
    AND (pay.ДатаЗакінчення IS NULL OR o.Period <= pay.ДатаЗакінчення)
WHERE o.Period >= '{START_DATE}'
"""

print("[INFO] Виконуємо SQL-запит...")
df = pd.read_sql(sql_query, con=engine)
print(f"[OK] Отримано {len(df)} рядків")

# === Перевірка дублікатів перед вставкою ===
key_fields = ['Recorder', 'LineNumber', 'Role']
duplicates = df[df.duplicated(subset=key_fields, keep=False)]

if not duplicates.empty:
    print("[DUPLICATE WARNING] Знайдено дублікати за ключовими полями:")
    print(duplicates[key_fields + ['Ном_Description', 'Period']].head(50))
    duplicates.to_csv("duplicate_rows.csv", index=False)
else:
    print("[OK] Дублікатів за ключовими полями немає.")

# === Очищення таблиці zp_sales_salary ===
with engine.begin() as conn:
    print("[DELETE] Очищення старих записів...")
    conn.exec_driver_sql("TRUNCATE TABLE petwealth.zp_sales_salary")

# === Завантаження нових даних ===
print("[LOAD] Завантажуємо дані у zp_sales_salary...")
df.to_sql(
    name='zp_sales_salary',
    con=engine,
    schema='petwealth',
    if_exists='append',
    index=False,
    chunksize=5000,
    method='multi'
)
print("[OK] Дані успішно завантажені!")
