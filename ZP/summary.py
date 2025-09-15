# -*- coding: utf-8 -*-
"""
Скрипт: salary_summary_aggregated.py (версія для Hetzнер)

Функція:
- Агрегує зарплату по змінах із таблиць:
    • zp_worktime (основна інформація по зміні)
    • zp_sales_salary (премії по групах продажів)
    • zp_collective_bonus (колективні премії)
- Розраховує зарплату за зміну на основі СтавкаГодина * DurationShift
- Очищає таблицю zp_summary через TRUNCATE
- Записує результат у zp_summary
- Працює для даних з START_DATE (за замовчуванням 2025-05-01)
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
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

# === Параметри БД (увага на назви змінних!) ===
DB_HOST = require_env("DB_HOST")
DB_PORT = int(require_env("DB_PORT"))
DB_USER = require_env("DB_USER")
DB_PASSWORD = require_env("DB_PASSWORD")
DB_DATABASE = require_env("DB_DATABASE")

# Екрануємо пароль для URI
DB_PASSWORD_ENC = quote_plus(DB_PASSWORD)

# === SQLAlchemy engine ===
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD_ENC}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}",
    connect_args={'charset': 'utf8mb4'}
)

# Єдиний старт періоду
START_DATE = os.getenv("START_DATE", "2025-05-01")

# 1️⃣ Основна таблиця zp_worktime
query_worktime = f"""
    SELECT
        shift_uuid,
        last_name AS Прізвище,
        date_shift AS ДатаЗміни,
        position AS Посада,
        department AS Відділення,
        shift_type AS ТипЗміни,
        duration_text AS ТривалістьЗміни,
        duration_hours AS DurationShift,
        СтавкаГодина
    FROM zp_worktime
    WHERE date_shift >= '{START_DATE}'
"""
df_worktime = pd.read_sql(query_worktime, con=engine)
df_worktime['Ставка'] = (df_worktime['СтавкаГодина'] * df_worktime['DurationShift']).round(2)

# 2️⃣ Премії з zp_sales_salary (фільтруємо за періодом через зв’язок із worktime)
sales_queries = {
    'Стоимость': 'Стоимость',
    'СтоимостьБезСкидок': 'СтоимостьБезСкидок',
    'ВаловийПрибуток': 'ВаловийПрибуток'
}
sales_pivots = {}

for label, field in sales_queries.items():
    query_sales = f"""
        SELECT
            s.shift_uuid,
            s.Ан_Description,
            SUM(s.{field} * s.ВідсотокПремії) AS Премія_{label}
        FROM zp_sales_salary s
        JOIN zp_worktime w ON w.shift_uuid = s.shift_uuid
        WHERE w.date_shift >= '{START_DATE}'
        GROUP BY s.shift_uuid, s.Ан_Description
    """
    df_sales = pd.read_sql(query_sales, con=engine)
    if df_sales.empty:
        sales_pivots[label] = pd.DataFrame(index=[], columns=[])
        continue
    pivot = df_sales.pivot_table(
        index='shift_uuid',
        columns='Ан_Description',
        values=f'Премія_{label}',
        fill_value=0
    )
    pivot.columns = [f"Премія{label}_{col}" for col in pivot.columns]
    pivot[f'Премія{label}_Всього'] = pivot.sum(axis=1)
    sales_pivots[label] = pivot

# 3️⃣ Колективні премії з zp_collective_bonus (також фільтруємо періодом через worktime)
bonus_queries = {
    'СтоимостьПремія': 'СтоимостьПремія',
    'СтоимостьБезСкидокПремія': 'СтоимостьБезСкидокПремія',
    'ВаловийПрибутокПремія': 'ВаловийПрибутокПремія'
}
bonus_pivots = {}

for label, field in bonus_queries.items():
    query_bonus = f"""
        SELECT
            b.`shift_uuid`,
            b.`АнЗП_Колективний` AS `Ан_Description`,
            SUM(b.`{field}`) AS `КолективнаПремія{label}`
        FROM `zp_collective_bonus` b
        JOIN `zp_worktime` w ON w.`shift_uuid` = b.`shift_uuid`
        WHERE w.`date_shift` >= '{START_DATE}'
        GROUP BY b.`shift_uuid`, b.`АнЗП_Колективний`
    """
    df_bonus = pd.read_sql(query_bonus, con=engine)
   
    if df_bonus.empty:
        bonus_pivots[label] = pd.DataFrame(index=[], columns=[])
        continue
    pivot = df_bonus.pivot_table(
        index='shift_uuid',
        columns='Ан_Description',
        values=f'КолективнаПремія{label}',
        fill_value=0
    )
    pivot.columns = [f"КолективнаПремія{label}_{col}" for col in pivot.columns]
    pivot[f'КолективнаПремія{label}_Всього'] = pivot.sum(axis=1)
    bonus_pivots[label] = pivot

# 4️⃣ Об’єднання всіх таблиць
summary_df = df_worktime.set_index('shift_uuid')
for pivot in sales_pivots.values():
    if len(pivot) > 0:
        summary_df = summary_df.join(pivot, how='left')
for pivot in bonus_pivots.values():
    if len(pivot) > 0:
        summary_df = summary_df.join(pivot, how='left')
summary_df = summary_df.fillna(0)

# 5️⃣ Обчислення фінальних колонок
summary_df['ВсьогоЗаЗмінуСтоимость'] = (
    summary_df['Ставка'] +
    summary_df.get('ПреміяСтоимость_Всього', 0) +
    summary_df.get('КолективнаПреміяСтоимостьПремія_Всього', 0)
)

summary_df['ВсьогоЗаЗмінуСтоимостьБезСкидок'] = (
    summary_df['Ставка'] +
    summary_df.get('ПреміяСтоимостьБезСкидок_Всього', 0) +
    summary_df.get('КолективнаПреміяСтоимостьБезСкидокПремія_Всього', 0)
)

summary_df['ВсьогоЗаЗмінуВаловийПрибуток'] = (
    summary_df['Ставка'] +
    summary_df.get('ПреміяВаловийПрибуток_Всього', 0) +
    summary_df.get('КолективнаПреміяВаловийПрибутокПремія_Всього', 0)
)

# 6️⃣ Підсумок у консоль
print("\n===============================")
print(f"Період від {START_DATE}")
print(f"По Стоимость: {summary_df['ВсьогоЗаЗмінуСтоимость'].sum():,.2f}")
print(f"По СтоимостьБезСкидок: {summary_df['ВсьогоЗаЗмінуСтоимостьБезСкидок'].sum():,.2f}")
print(f"По ВаловийПрибуток: {summary_df['ВсьогоЗаЗмінуВаловийПрибуток'].sum():,.2f}")
print("===============================\n")

# 7️⃣ TRUNCATE таблиці zp_summary
with engine.begin() as conn:
    conn.exec_driver_sql("TRUNCATE TABLE zp_summary")

# 8️⃣ Видаляємо СтавкаГодина, запис у БД
summary_df = summary_df.drop(columns=['СтавкаГодина'], errors='ignore')
summary_df.reset_index().to_sql(
    'zp_summary',
    con=engine,
    if_exists='append',
    index=False,
    chunksize=5000,
    method='multi'
)

print("[OK] Дані успішно завантажено у таблицю zp_summary.")
