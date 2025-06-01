"""
Скрипт для синхронізації таблиці рівнів співробітників між Google Sheets і MySQL.

1️⃣ Зчитує дані з Google Sheets (фкт_РівніСпівробітників).
2️⃣ Виконує триммування текстових полів і підміняє NULL на '' для уникнення дублів.
3️⃣ Перевіряє дублі в Google Sheets (Прізвище, ДатаПочатку, Посада, Відділення, Рівень):
    - Якщо є дублікати — зупиняє виконання та виводить їх у консоль.
4️⃣ Проставляє ДатаЗакінчення для попереднього рівня співробітника (якщо є перехід на новий).
5️⃣ Оновлює Google Sheets із датами закінчення.
6️⃣ Завантажує дані у таблицю zp_фктРівніСпівробітників в MySQL:
    - INSERT ... ON DUPLICATE KEY UPDATE (оновлює або додає рядок).
7️⃣ Логування дій у консоль:
    - Попередження про дублікати.
    - Нові та оновлені записи.
    - Закриття рівнів.
"""


import os
import pymysql
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime, timedelta

# === Авторизація в Google Sheets ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

SPREADSHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_РівніСпівробітників"
RANGE_NAME = f"{SHEET_NAME}!A1:Z"
TOKEN_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Google/token.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly"
]

creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
service = build("sheets", "v4", credentials=creds)

# === Логування у консоль ===
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# === Отримуємо дані з Google Sheets ===
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
values = result.get("values", [])

if not values:
    log_message("❌ Дані не знайдено!")
    exit()

# === Обробка даних у DataFrame ===
header = values[0]
data = values[1:]
df = pd.DataFrame(data, columns=header)

# Перетворення дат
df["ДатаПочатку"] = pd.to_datetime(df["ДатаПочатку"], dayfirst=True, errors="coerce")
df["ДатаЗакінчення"] = pd.to_datetime(df["ДатаЗакінчення"], dayfirst=True, errors="coerce")

# === Триммування текстових полів і підміна NULL на '' ===
def clean_string(value):
    return value.strip() if isinstance(value, str) else ""

text_columns = ["Прізвище", "Посада", "Відділення", "Рівень"]
for col in text_columns:
    df[col] = df[col].apply(lambda x: clean_string(x) if pd.notnull(x) else "")

# === Перевірка на дублікати у Google Sheets ===
duplicate_mask = df.duplicated(subset=["Прізвище", "ДатаПочатку", "Посада", "Відділення", "Рівень"], keep=False)
if duplicate_mask.any():
    log_message("⚠️ В Google Sheets виявлено дублікати:")
    duplicates = df[duplicate_mask]
    for _, row in duplicates.iterrows():
        log_message(f"  - {row['Прізвище']} | {row['ДатаПочатку'].date() if pd.notnull(row['ДатаПочатку']) else ''} | "
                    f"{row['Посада']} | {row['Відділення']} | {row['Рівень']}")
    log_message("❌ Дані не будуть завантажені у БД до виправлення дублікатів у Google Sheets.")
    exit()

# === Сортування для обробки рівнів ===
df.sort_values(
    by=["Прізвище", "Посада", "Відділення", "ДатаПочатку"],
    inplace=True
)

# === Розставлення ДатаЗакінчення ===
prev_row = None
for idx, row in df.iterrows():
    if pd.isnull(row["ДатаПочатку"]):
        continue

    key = (row["Прізвище"], row["Посада"], row["Відділення"])
    if prev_row is not None:
        prev_key = (prev_row["Прізвище"], prev_row["Посада"], prev_row["Відділення"])
        if key == prev_key:
            if pd.isnull(prev_row["ДатаЗакінчення"]):
                if row["ДатаПочатку"] > prev_row["ДатаПочатку"]:
                    df.at[prev_row.name, "ДатаЗакінчення"] = row["ДатаПочатку"] - timedelta(days=1)
                    log_message(f"🔄 Закрито рівень для: {prev_row['Прізвище']} | {prev_row['Посада']} | "
                                f"{prev_row['Відділення']} | {prev_row['ДатаПочатку'].date()} → "
                                f"{df.at[prev_row.name, 'ДатаЗакінчення'].date()}")
    if pd.isnull(row["ДатаЗакінчення"]):
        prev_row = row
    else:
        prev_row = None

# === Оновлюємо Google Sheets ===
updated_values = [header]
for _, row in df.iterrows():
    updated_row = []
    for col in header:
        value = row.get(col, "")
        if isinstance(value, pd.Timestamp):
            value = value.strftime("%d.%m.%Y")
        elif pd.isnull(value):
            value = ""
        updated_row.append(value)
    updated_values.append(updated_row)

sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME,
    valueInputOption="RAW",
    body={"values": updated_values}
).execute()

log_message("✅ Google Sheet оновлено!")

# === Завантаження у MySQL ===
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

table_name = "zp_фктРівніСпівробітників"
inserted_rows = 0
updated_rows = 0

for _, row in df.iterrows():
    sql = f"""
        INSERT INTO {table_name}
        (`Прізвище`, `ДатаПочатку`, `ДатаЗакінчення`, `Посада`, `Відділення`, `Рівень`)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `ДатаЗакінчення` = VALUES(`ДатаЗакінчення`),
            `Рівень` = VALUES(`Рівень`)
    """
    cursor.execute(
        sql,
        (
            row["Прізвище"],
            row["ДатаПочатку"].strftime("%Y-%m-%d") if not pd.isnull(row["ДатаПочатку"]) else None,
            row["ДатаЗакінчення"].strftime("%Y-%m-%d") if not pd.isnull(row["ДатаЗакінчення"]) else None,
            row["Посада"],
            row["Відділення"],
            row["Рівень"]
        )
    )
    if cursor.rowcount == 1:
        inserted_rows += 1
        log_message(f"➕ Додано: {row['Прізвище']} | {row['ДатаПочатку'].date()} | {row['Посада']} | "
                    f"{row['Відділення']} | {row['Рівень']}")
    elif cursor.rowcount == 2:
        updated_rows += 1
        log_message(f"✏️ Оновлено: {row['Прізвище']} | {row['ДатаПочатку'].date()} | {row['Посада']} | "
                    f"{row['Відділення']} | {row['Рівень']}")

conn.commit()
cursor.close()
conn.close()

log_message(f"✅ Завантаження завершено: {inserted_rows} додано, {updated_rows} оновлено.")
