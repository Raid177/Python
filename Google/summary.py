"""
Скрипт: salary_summary_aggregated.py

Функція:
- Агрегує зарплату по змінах із таблиць:
    • zp_worktime (основна інформація по зміні)
    • zp_sales_salary (премії по групах продажів)
    • zp_collective_bonus (колективні премії)
- Розраховує зарплату за зміну на основі СтавкаГодина * DurationShift
- Очищає таблицю zp_summary через TRUNCATE
- Записує результат у zp_summary
- Працює для даних з 2025-05-01
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

# 1️⃣ Основна таблиця zp_worktime
query_worktime = """
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
    WHERE date_shift >= '2025-05-01'
"""
df_worktime = pd.read_sql(query_worktime, con=engine)

# Перерахунок Ставки за фактично відпрацьований час
df_worktime['Ставка'] = df_worktime['СтавкаГодина'] * df_worktime['DurationShift']

# 2️⃣ Премії з zp_sales_salary
sales_queries = {
    'Стоимость': 'Стоимость',
    'СтоимостьБезСкидок': 'СтоимостьБезСкидок',
    'ВаловийПрибуток': 'ВаловийПрибуток'
}
sales_pivots = {}

for label, field in sales_queries.items():
    query_sales = f"""
        SELECT
            shift_uuid,
            Ан_Description,
            SUM({field} * ВідсотокПремії) AS Премія_{label}
        FROM zp_sales_salary
        GROUP BY shift_uuid, Ан_Description
    """
    df_sales = pd.read_sql(query_sales, con=engine)
    pivot = df_sales.pivot_table(
        index='shift_uuid',
        columns='Ан_Description',
        values=f'Премія_{label}',
        fill_value=0
    )
    pivot.columns = [f"Премія{label}_{col}" for col in pivot.columns]
    pivot[f'Премія{label}_Всього'] = pivot.sum(axis=1)
    sales_pivots[label] = pivot

# 3️⃣ Колективні премії з zp_collective_bonus
bonus_queries = {
    'СтоимостьПремія': 'СтоимостьПремія',
    'СтоимостьБезСкидокПремія': 'СтоимостьБезСкидокПремія',
    'ВаловийПрибутокПремія': 'ВаловийПрибутокПремія'
}
bonus_pivots = {}

for label, field in bonus_queries.items():
    query_bonus = f"""
        SELECT
            shift_uuid,
            АнЗП_Колективний AS Ан_Description,
            SUM({field}) AS КолективнаПремія{label}
        FROM zp_collective_bonus
        GROUP BY shift_uuid, Ан_Description
    """
    df_bonus = pd.read_sql(query_bonus, con=engine)
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
    summary_df = summary_df.join(pivot, how='left')
for pivot in bonus_pivots.values():
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

# 6️⃣ Консольний підсумок
total_salary_stoimost = summary_df['ВсьогоЗаЗмінуСтоимость'].sum()
total_salary_stoimost_bez_skidok = summary_df['ВсьогоЗаЗмінуСтоимостьБезСкидок'].sum()
total_salary_valovyi_prybutok = summary_df['ВсьогоЗаЗмінуВаловийПрибуток'].sum()

print("\n===============================")
print("Загальна зарплата за весь період:")
print(f"По Стоимость: {total_salary_stoimost:,.2f}")
print(f"По СтоимостьБезСкидок: {total_salary_stoimost_bez_skidok:,.2f}")
print(f"По ВаловийПрибуток: {total_salary_valovyi_prybutok:,.2f}")
print("===============================\n")

# 7️⃣ Очистка таблиці zp_summary через TRUNCATE
with engine.begin() as conn:
    truncate_sql = text("TRUNCATE TABLE zp_summary")
    conn.execute(truncate_sql)

# 8️⃣ Видаляємо поле СтавкаГодина перед записом у БД
if 'СтавкаГодина' in summary_df.columns:
    summary_df = summary_df.drop(columns=['СтавкаГодина'])

# 9️⃣ Запис у таблицю zp_summary
summary_df.reset_index().to_sql(
    'zp_summary',
    con=engine,
    if_exists='append',
    index=False
)

print("[OK] Дані успішно завантажено у таблицю zp_summary.")
