"""
Цей скрипт імпортує дані з Google Sheets у таблицю zp_фктУмовиОплати:
1️⃣ Завантажує дані.
2️⃣ Перевіряє дублікати.
3️⃣ Автоматично проставляє ДатаЗакінчення для попередніх правил, якщо з'являється новий рядок із новішою датою.
4️⃣ Формує Rule_ID для груп правил.
5️⃣ Завантажує в Google Sheets і в MySQL (truncate + insert).
"""

import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# === Налаштування ===
TOKEN_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Google/token.json"
SPREADSHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_УмовиОплати"
RANGE_NAME = f"{SHEET_NAME}!A1:Z"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

print(f"🚀 Скрипт запущено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# === Крок 1. Отримати дані з Google Sheets ===
result = sheet.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME
).execute()
values = result.get('values', [])

if not values or len(values) < 2:
    print("❌ Google Sheet порожній або відсутні дані.")
    exit(1)

header_len = len(values[0])
data_rows = []
for row in values[1:]:
    while len(row) < header_len:
        row.append('')
    data_rows.append(row)

df = pd.DataFrame(data_rows, columns=values[0])
print(f"✅ Отримано {len(df)} рядків із Google Sheets.")

# === Обробка дат ===
df['ДатаПочатку'] = pd.to_datetime(df['ДатаПочатку'], dayfirst=True, errors='coerce')
df['ДатаЗакінчення'] = pd.to_datetime(df['ДатаЗакінчення'], dayfirst=True, errors='coerce')

# === Сортування для Rule_ID ===
df.sort_values(by=['Посада', 'Відділення', 'Рівень', 'ТипЗміни', 'Прізвище', 'АнЗП_Колективний', 'АнЗП', 'ДатаПочатку'], inplace=True)

# === Проставляємо ДатаЗакінчення ===
changed_rows = 0
for idx in range(1, len(df)):
    curr = df.iloc[idx]
    prev = df.iloc[idx - 1]
    if (
        curr['Посада'] == prev['Посада'] and
        curr['Відділення'] == prev['Відділення'] and
        curr['Рівень'] == prev['Рівень'] and
        curr['ТипЗміни'] == prev['ТипЗміни'] and
        curr['Прізвище'] == prev['Прізвище'] and
        curr['АнЗП_Колективний'] == prev['АнЗП_Колективний'] and
        curr['АнЗП'] == prev['АнЗП']
    ):
        if pd.isna(prev['ДатаЗакінчення']) or prev['ДатаЗакінчення'] == '':
            new_end_date = curr['ДатаПочатку'] - timedelta(days=1)
            df.at[df.index[idx - 1], 'ДатаЗакінчення'] = new_end_date.strftime('%d.%m.%Y')
            changed_rows += 1

print(f"✅ Всього змінено ДатаЗакінчення у {changed_rows} рядках.")

# === Присвоюємо Rule_ID ===
current_rule_id = 1
df['Rule_ID'] = 0
df.at[df.index[0], 'Rule_ID'] = current_rule_id

for idx in range(1, len(df)):
    curr = df.iloc[idx]
    prev = df.iloc[idx - 1]
    if (
        curr['Посада'] == prev['Посада'] and
        curr['Відділення'] == prev['Відділення'] and
        curr['Рівень'] == prev['Рівень'] and
        curr['ТипЗміни'] == prev['ТипЗміни'] and
        curr['Прізвище'] == prev['Прізвище'] and
        curr['АнЗП_Колективний'] == prev['АнЗП_Колективний']
    ):
        df.at[df.index[idx], 'Rule_ID'] = current_rule_id
    else:
        current_rule_id += 1
        df.at[df.index[idx], 'Rule_ID'] = current_rule_id

print("✅ Rule_ID присвоєно для всіх рядків.")

# === Форматуємо дати для завантаження назад у Google Sheets ===
df['ДатаПочатку'] = df['ДатаПочатку'].dt.strftime('%d.%m.%Y')
df['ДатаЗакінчення'] = df['ДатаЗакінчення'].apply(lambda x: x.strftime('%d.%m.%Y') if pd.notna(x) and x != '' else '')

# === Оновлюємо дані у Google Sheets ===
values_to_upload = [list(df.columns)] + df.values.tolist()
sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME,
    valueInputOption='RAW',
    body={'values': values_to_upload}
).execute()
print("✅ Дані оновлені у Google Sheets.")

# === Крок 4. Завантажуємо у БД ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

connection = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)

df['ДатаПочатку'] = pd.to_datetime(df['ДатаПочатку'], dayfirst=True, errors='coerce')
df['ДатаЗакінчення'] = pd.to_datetime(df['ДатаЗакінчення'], dayfirst=True, errors='coerce')

with connection.cursor() as cursor:
    cursor.execute("TRUNCATE TABLE zp_фктУмовиОплати")
    insert_sql = """
        INSERT INTO zp_фктУмовиОплати (
            Rule_ID, ДатаПочатку, ДатаЗакінчення, Посада, Відділення, Рівень,
            ТипЗміни, Прізвище, СтавкаЗміна, АнЗП,
            Ан_Призначив, Ан_Виконав, АнЗП_Колективний, Ан_Колективний
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    inserted_rows = 0
    for _, row in df.iterrows():
        cursor.execute(insert_sql, (
            row['Rule_ID'],
            row['ДатаПочатку'] if pd.notna(row['ДатаПочатку']) else None,
            row['ДатаЗакінчення'] if pd.notna(row['ДатаЗакінчення']) else None,
            row['Посада'],
            row['Відділення'],
            row['Рівень'],
            row['ТипЗміни'],
            row['Прізвище'],
            float(str(row.get('СтавкаЗміна', 0)).replace(',', '.')) if row.get('СтавкаЗміна', '') else 0,
            row.get('АнЗП', ''),
            float(str(row.get('Ан_Призначив', 0)).replace(',', '.')) if row.get('Ан_Призначив', '') else 0,
            float(str(row.get('Ан_Виконав', 0)).replace(',', '.')) if row.get('Ан_Виконав', '') else 0,
            row.get('АнЗП_Колективний', ''),
            float(str(row.get('Ан_Колективний', 0)).replace(',', '.')) if row.get('Ан_Колективний', '') else 0
        ))
        inserted_rows += 1

    connection.commit()

print(f"✅ Дані перезаписані у БД (zp_фктУмовиОплати). Вставлено {inserted_rows} рядків.")
