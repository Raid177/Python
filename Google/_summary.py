"""
Скрипт: salary_summary_aggregated.py

Функція:
- Агрегує зарплату по змінах із таблиць:
    • zp_worktime (основна інформація по зміні)
    • zp_sales_salary (премії по групах продажів)
    • zp_collective_bonus (колективні премії)
- Об’єднує дані в єдину фінальну таблицю з унікальним рядком на кожну зміну
- Виводить результат у файл Excel та консоль
- Працює для даних з 2025-05-01

Інструкції:
1. Налаштуйте змінні .env для доступу до БД.
2. Запустіть скрипт: python salary_summary_aggregated.py
3. Результат буде у файлі 'salary_summary.xlsx' та у консолі.
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
        СтавкаЗміна AS Ставка
    FROM zp_worktime
    WHERE date_shift >= '2025-05-01'
"""
df_worktime = pd.read_sql(query_worktime, con=engine)

# 2️⃣ Премії з zp_sales_salary (по групах)
query_sales = """
    SELECT
        shift_uuid,
        Ан_Description,
        SUM(Стоимость * ВідсотокПремії) AS Премія
    FROM zp_sales_salary
    WHERE Role = 'Призначив'
    GROUP BY shift_uuid, Ан_Description
"""
df_sales = pd.read_sql(query_sales, con=engine)
sales_pivot = df_sales.pivot_table(
    index='shift_uuid',
    columns='Ан_Description',
    values='Премія',
    fill_value=0
)
sales_pivot.columns = [f"Премія_{col}" for col in sales_pivot.columns]
sales_pivot['Премія_Всього'] = sales_pivot.sum(axis=1)

# 3️⃣ Колективні премії з zp_collective_bonus
query_bonus = """
    SELECT
        shift_uuid,
        АнЗП_Колективний AS Ан_Description,
        SUM(СтоимостьПремія) AS КолективнаПремія
    FROM zp_collective_bonus
    GROUP BY shift_uuid, Ан_Description
"""
df_bonus = pd.read_sql(query_bonus, con=engine)
bonus_pivot = df_bonus.pivot_table(
    index='shift_uuid',
    columns='Ан_Description',
    values='КолективнаПремія',
    fill_value=0
)
bonus_pivot.columns = [f"КолективнаПремія_{col}" for col in bonus_pivot.columns]
bonus_pivot['КолективнаПремія_Всього'] = bonus_pivot.sum(axis=1)

# 4️⃣ Об’єднання всіх таблиць
summary_df = df_worktime.set_index('shift_uuid')\
    .join(sales_pivot, how='left')\
    .join(bonus_pivot, how='left')

summary_df = summary_df.fillna(0)

# 5️⃣ Обчислення ВсьогоЗаЗміну
summary_df['ВсьогоЗаЗміну'] = summary_df['Ставка'] + summary_df['Премія_Всього'] + summary_df['КолективнаПремія_Всього']

# 6️⃣ Експорт у Excel
file_name = 'salary_summary.xlsx'
summary_df.reset_index().to_excel(file_name, index=False)
print(f"✅ Дані успішно експортовано до Excel: {file_name}")

# 7️⃣ Вивід у консоль
print("🏁 Результати агрегування зарплати по змінах:")
print(summary_df.reset_index())
