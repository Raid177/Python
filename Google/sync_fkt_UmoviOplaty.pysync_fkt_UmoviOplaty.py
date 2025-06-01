import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# === Крок 1. Авторизація Google Sheets ===
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

# === Крок 1. Отримати дані з Google Sheets ===
result = sheet.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME
).execute()
values = result.get('values', [])

if not values or len(values) < 2:
    print("❌ Google Sheet порожній або відсутні дані.")
    exit(1)

# Фіксуємо, щоб усі рядки мали однакову кількість колонок
header_len = len(values[0])
data_rows = []

for row in values[1:]:
    while len(row) < header_len:
        row.append('')
    data_rows.append(row)

df = pd.DataFrame(data_rows, columns=values[0])
print(f"✅ Отримано {len(df)} рядків із Google Sheets.")

# === Перевірка на дублікати у DataFrame ===
duplicate_mask = df.duplicated(
    subset=['ДатаПочатку', 'Посада', 'Відділення', 'Рівень', 'ТипЗміни', 'Прізвище', 'АнЗП', 'АнЗП_Колективний'],
    keep=False
)
duplicates = df[duplicate_mask]

if not duplicates.empty:
    print("❗️ Знайдено дублікати у даних:")
    print(duplicates[['ДатаПочатку', 'Посада', 'Відділення', 'Рівень', 'ТипЗміни', 'Прізвище', 'АнЗП', 'АнЗП_Колективний']])

# === Крок 2. Обробка дат ===
df['ДатаПочатку'] = pd.to_datetime(df['ДатаПочатку'], dayfirst=True, errors='coerce')
df['ДатаЗакінчення'] = pd.to_datetime(df['ДатаЗакінчення'], dayfirst=True, errors='coerce')

df.sort_values(by=['Прізвище', 'Посада', 'Відділення', 'Рівень', 'ТипЗміни', 'ДатаПочатку'], inplace=True)

# === Додаємо ДатаЗакінчення лише там, де треба ===
for idx in range(1, len(df)):
    curr = df.iloc[idx]
    prev = df.iloc[idx - 1]

    if (
        curr['Посада'] == prev['Посада'] and
        curr['Відділення'] == prev['Відділення'] and
        curr['Рівень'] == prev['Рівень'] and
        curr['ТипЗміни'] == prev['ТипЗміни'] and
        curr['Прізвище'] == prev['Прізвище'] and
        curr['АнЗП'] == prev['АнЗП'] and
        curr['АнЗП_Колективний'] == prev['АнЗП_Колективний']
    ):
        if pd.isna(prev['ДатаЗакінчення']) or prev['ДатаЗакінчення'] == '':
            new_end_date = curr['ДатаПочатку'] - timedelta(days=1)
            df.at[df.index[idx - 1], 'ДатаЗакінчення'] = new_end_date.strftime('%d.%m.%Y')

# Перетворюємо дати у формат dd.mm.yyyy
df['ДатаПочатку'] = df['ДатаПочатку'].dt.strftime('%d.%m.%Y')
df['ДатаЗакінчення'] = df['ДатаЗакінчення'].apply(lambda x: x.strftime('%d.%m.%Y') if pd.notna(x) and x != '' else '')

# === Крок 3. Оновлюємо дані у Google Sheets (без очищення та нових рядків) ===
values_to_upload = [list(df.columns)] + df.values.tolist()

sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME,
    valueInputOption='RAW',
    body={'values': values_to_upload}
).execute()
print("✅ Дані оновлені у Google Sheets (без зміни структури).")

# === Крок 4. Завантажуємо у БД (через TRUNCATE) ===
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
            ДатаПочатку, ДатаЗакінчення, Посада, Відділення, Рівень,
            ТипЗміни, Прізвище, СтавкаЗміна, СтавкаГодина, АнЗП,
            Ан_Призначив, Ан_Виконав, АнЗП_Колективний, Ан_Колективний
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_sql, (
            row['ДатаПочатку'] if pd.notna(row['ДатаПочатку']) else None,
            row['ДатаЗакінчення'] if pd.notna(row['ДатаЗакінчення']) else None,
            row['Посада'],
            row['Відділення'],
            row['Рівень'],
            row['ТипЗміни'],
            row['Прізвище'],
            float(str(row.get('СтавкаЗміна', 0)).replace(',', '.')) if row.get('СтавкаЗміна', '') else 0,
            float(str(row.get('СтавкаГодина', 0)).replace(',', '.')) if row.get('СтавкаГодина', '') else 0,
            row.get('АнЗП', ''),
            float(str(row.get('Ан_Призначив', 0)).replace(',', '.')) if row.get('Ан_Призначив', '') else 0,
            float(str(row.get('Ан_Виконав', 0)).replace(',', '.')) if row.get('Ан_Виконав', '') else 0,
            row.get('АнЗП_Колективний', ''),
            float(str(row.get('Ан_Колективний', 0)).replace(',', '.')) if row.get('Ан_Колективний', '') else 0
        ))

    connection.commit()

print("✅ Дані перезаписані у БД (zp_фктУмовиОплати).")
