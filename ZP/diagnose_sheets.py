# Отримуємо співробітників з Єноту. Звіряємо з графіком. Якщо неславабогу - закреслюєм. Вносимо в БД


import os
import requests
import gspread
import pymysql
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from datetime import datetime
import time
import re

# === Налаштування ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

# Авторизація Google Sheets через token.json
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
script_dir = os.path.dirname(os.path.abspath(__file__))
token_path = os.path.join(script_dir, "token.json")
creds = Credentials.from_authorized_user_file(token_path, SCOPES)
if creds.expired and creds.refresh_token:
    creds.refresh(Request())

client = gspread.authorize(creds)
service = build('sheets', 'v4', credentials=creds)

# Підключення до БД
connection = pymysql.connect(
    host=os.getenv("DB_HOST_Serv"),
    port=int(os.getenv("DB_PORT_Serv", 3306)),
    user=os.getenv("DB_USER_Serv"),
    password=os.getenv("DB_PASSWORD_Serv"),
    database=os.getenv("DB_DATABASE_Serv"),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor
)
cursor = connection.cursor()

# === Функції для форматування тексту ===
def a1_to_gridrange(a1_notation, sheet_id):
    match = re.match(r"([A-Z]+)(\d+)", a1_notation)
    if not match:
        raise ValueError("Invalid A1 notation")
    col, row = match.groups()
    col_idx = sum((ord(char) - 64) * (26 ** i) for i, char in enumerate(reversed(col))) - 1
    return {
        "sheetId": sheet_id,
        "startRowIndex": int(row) - 1,
        "endRowIndex": int(row),
        "startColumnIndex": col_idx,
        "endColumnIndex": col_idx + 1,
    }

def mark_strikethrough(spreadsheet_id, sheet_id, cells, enable=True):
    requests = []
    for cell in cells:
        requests.append({
            "repeatCell": {
                "range": a1_to_gridrange(cell, sheet_id),
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "strikethrough": enable
                        }
                    }
                },
                "fields": "userEnteredFormat.textFormat.strikethrough"
            }
        })
    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

print("\n[LOG] Початок перевірки графіка —", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# === 1. Отримати дані з Єнота ===
response = requests.get(
    "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262/odata/standard.odata/Catalog_ФизическиеЛица?$select=Ref_Key,Code,Description&$filter=IsFolder eq false&$format=json",
    auth=(os.getenv("ODATA_USER"), os.getenv("ODATA_PASSWORD"))
)
response.raise_for_status()
data = response.json()['value']
print(f"[LOG] Отримано {len(data)} співробітників з Єнота")

# === 2. Оновити дов_Співробітники ===
staff_ws = client.open("zp_PetWealth").worksheet("дов_Співробітники")
staff_data = staff_ws.get_all_records()
manual_links = {row['Ref_Key']: row['Графік'].strip() for row in staff_data if row['Ref_Key']}

staff_map = {
    entry['Ref_Key']: {
        'ПІБ': entry['Description'].strip(),
        'Code': entry['Code'],
        'Графік': manual_links.get(entry['Ref_Key'], '')
    } for entry in data
}

new_rows = []
for ref, info in staff_map.items():
    графік = manual_links.get(ref, '')
    new_rows.append([info['ПІБ'], графік, info['Code'], ref])

staff_ws.clear()
header = ["ПІБ", "Графік", "Code", "Ref_Key"]
staff_ws.update([header] + new_rows)

# === 2б. Зберегти дов_Співробітники в БД ===
print("[LOG] Оновлюємо таблицю 'zp_довСпівробітники' в БД...")

# Створення таблиці, якщо ще немає
cursor.execute("""
    CREATE TABLE IF NOT EXISTS zp_довСпівробітники (
        ПІБ VARCHAR(255),
        Графік VARCHAR(255),
        Code VARCHAR(64),
        Ref_Key CHAR(36) PRIMARY KEY
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""")

# Очищення перед вставкою
cursor.execute("DELETE FROM zp_довСпівробітники")

# Масив для вставки
db_rows = [(row[0], row[1], row[2], row[3]) for row in new_rows]

# Вставка даних
cursor.executemany("""
    INSERT INTO zp_довСпівробітники (ПІБ, Графік, Code, Ref_Key)
    VALUES (%s, %s, %s, %s)
""", db_rows)
connection.commit()

print(f"[LOG] Дані записано в таблицю zp_довСпівробітники — {len(db_rows)} рядків")

print(f"[LOG] Оновлено дов_Співробітники — {len(new_rows)} рядків")

# === 3. Перевірка графіка ===
spreadsheet = client.open("zp_PetWealth")
schedule_ws = spreadsheet.worksheet("Графік")
schedule_data = schedule_ws.get_all_values()
valid_names = {row[1].strip() for row in new_rows if row[1].strip()}
sheet_id = schedule_ws.id
spreadsheet_id = schedule_ws.spreadsheet.id

print("[LOG] Починаємо перевірку клітинок з H2 по AL...")
invalid_count = 0
fixed_count = 0
insert_errors = []
resolve_errors = []
strike_on = []
strike_off = []

for row_idx, row in enumerate(schedule_data[1:], start=2):
    month_year = row[0].strip()
    idx = row[6].strip()

    for col_idx in range(8, 39):
        name = row[col_idx-1].strip()
        cell = gspread.utils.rowcol_to_a1(row_idx, col_idx)
        if not name:
            continue

        day_number = col_idx - 7

        try:
            if name not in valid_names:
                strike_on.append(cell)
                insert_errors.append((month_year, idx, day_number, cell, name, "Не знайдено у довіднику"))
                invalid_count += 1
            else:
                strike_off.append(cell)
                resolve_errors.append((month_year, idx, day_number))
                fixed_count += 1
        except Exception as e:
            print(f"[ERR] {cell}: Помилка при обробці — {e}")
            continue

# Застосування стилю
mark_strikethrough(spreadsheet_id, sheet_id, strike_on, enable=True)
mark_strikethrough(spreadsheet_id, sheet_id, strike_off, enable=False)

# Пакетне оновлення БД
if insert_errors:
    cursor.executemany("""
        INSERT INTO zp_log_schedule_errors (month_year, idx, day_number, cell, value, comment)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            value = VALUES(value),
            comment = VALUES(comment),
            resolved_at = NULL
    """, insert_errors)

if resolve_errors:
    cursor.executemany("""
        UPDATE zp_log_schedule_errors
        SET resolved_at = NOW()
        WHERE month_year = %s AND idx = %s AND day_number = %s AND resolved_at IS NULL
    """, resolve_errors)

connection.commit()
cursor.close()
connection.close()

print(f"[LOG] Перевірка завершена. Некоректних клітинок: {invalid_count}, очищено форматів: {fixed_count}\n")
