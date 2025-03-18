import pymysql
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, time
from meteostat import Point, Hourly

# Завантаження параметрів з .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# Підключення до БД
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
cursor = conn.cursor()

# Перевіряємо останню доступну дату
cursor.execute("SELECT MAX(date) FROM weather_kyiv")
last_date = cursor.fetchone()[0]

# Якщо в БД немає даних - беремо з 1 січня 2024 року
if last_date is None:
    start_date = datetime(2024, 1, 1)
else:
    start_date = last_date + timedelta(days=1)

# Встановлюємо кінцеву дату (сьогодні)
end_date = datetime.today()

# Якщо немає нових даних для завантаження - виходимо
if start_date >= end_date:
    print("✅ Дані в БД вже актуальні, оновлення не потрібно.")
    cursor.close()
    conn.close()
    exit()

# Координати Києва
kyiv = Point(50.4501, 30.5234)

# Отримуємо погодинні дані
hourly_data = Hourly(kyiv, start_date, end_date)
hourly_data = hourly_data.fetch()

# Перевіряємо, чи є дані
if hourly_data.empty:
    print("❌ Не вдалося отримати погодинні дані. Перевірте API.")
    cursor.close()
    conn.close()
    exit()

# Форматуємо дані
hourly_data.reset_index(inplace=True)
hourly_data['Дата'] = hourly_data['time'].dt.date
hourly_data['Година'] = hourly_data['time'].dt.time

# Фільтруємо денні (9:00–21:00) та нічні (21:00–9:00) значення
day_filter = (hourly_data['Година'] >= time(9, 0)) & (hourly_data['Година'] < time(21, 0))
night_filter = (hourly_data['Година'] >= time(21, 0)) | (hourly_data['Година'] < time(9, 0))

# Розрахунок середніх температур
daily_avg_temp = hourly_data.groupby('Дата')['temp'].mean().reset_index().rename(columns={'temp': 'avg_temp'})
day_avg_temp = hourly_data[day_filter].groupby('Дата')['temp'].mean().reset_index().rename(columns={'temp': 'day_temp'})
night_avg_temp = hourly_data[night_filter].groupby('Дата')['temp'].mean().reset_index().rename(columns={'temp': 'night_temp'})

# Об'єднання всіх значень у один DataFrame
df = daily_avg_temp.merge(day_avg_temp, on='Дата', how='left').merge(night_avg_temp, on='Дата', how='left')

# Використовуємо значення попереднього дня, якщо відсутні денні або нічні значення
df['day_temp'] = df['day_temp'].fillna(method='ffill')  # Беремо значення з попереднього дня
df['night_temp'] = df['night_temp'].fillna(method='ffill')  # Те ж саме для нічної температури

# Якщо попередніх значень немає - використовуємо середньодобову температуру
df['day_temp'].fillna(df['avg_temp'], inplace=True)
df['night_temp'].fillna(df['avg_temp'], inplace=True)

# Якщо і середньодобова температура відсутня - замінюємо на None для MySQL
df = df.where(pd.notnull(df), None)

# ДОДАТКОВА ПЕРЕВІРКА: ВИВЕДЕННЯ NaN (Якщо є - код зупиниться)
if df.isnull().values.any():
    print("❌ УВАГА! У DataFrame залишилися NaN. Дані не будуть завантажені в БД!")
    print(df[df.isnull().any(axis=1)])  # Вивести рядки з NaN
    cursor.close()
    conn.close()
    exit()

# SQL-запит для вставки даних
sql = """
INSERT INTO weather_kyiv (date, avg_temp, day_temp, night_temp)
VALUES (%s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    avg_temp = VALUES(avg_temp),
    day_temp = VALUES(day_temp),
    night_temp = VALUES(night_temp);
"""

# Завантаження даних у БД
for _, row in df.iterrows():
    cursor.execute(sql, (row['Дата'], row['avg_temp'], row['day_temp'], row['night_temp']))

# Фіксуємо зміни
conn.commit()
cursor.close()
conn.close()

print(f"✅ Успішно оновлено {len(df)} записів у БД!")
