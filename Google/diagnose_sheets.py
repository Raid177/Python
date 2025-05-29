import os
import requests
import gspread
import pymysql
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from gspread_formatting import *
from gspread_formatting.dataframe import format_cell_ranges
from datetime import datetime
import time

# === Налаштування ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

# Авторизація Google Sheets через token.json
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive.readonly'
]
script_dir = os.path.dirname(os.path.abspath(__file__))
token_path = os.path.join(script_dir, "token.json")
creds = Credentials.from_authorized_user_file(token_path, SCOPES)
if creds.expired and creds.refresh_token:
    creds.refresh(Request())

client = gspread.authorize(creds)

# Підключення до БД
connection = pymysql.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_DATABASE"),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor
)
cursor = connection.cursor()

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
staff_ws.append_row(["ПІБ", "Графік", "Code", "Ref_Key"])
for row in new_rows:
    staff_ws.append_row(row)
print(f"[LOG] Оновлено дов_Співробітники — {len(new_rows)} рядків")

# === 3. Перевірка графіка ===
schedule_ws = client.open("zp_PetWealth").worksheet("Графік")
schedule_data = schedule_ws.get_all_values()
valid_names = {row[1].strip() for row in new_rows if row[1].strip()}

print("[LOG] Починаємо перевірку клітинок з H2 по AL...")
invalid_count = 0
fixed_count = 0

insert_errors = []
resolve_errors = []
style_updates = []

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
                fmt = cellFormat(borders=Borders(
                    left=Border("SOLID_THICK", Color(1, 0, 0)),
                    right=Border("SOLID_THICK", Color(1, 0, 0)),
                    top=Border("SOLID_THICK", Color(1, 0, 0)),
                    bottom=Border("SOLID_THICK", Color(1, 0, 0))
                ))
                style_updates.append((cell, fmt))
                insert_errors.append((month_year, idx, day_number, cell, name, "Не знайдено у довіднику"))
                invalid_count += 1
            else:
                fmt_clear_borders = cellFormat(
                    borders=Borders(
                        left=None,
                        right=None,
                        top=None,
                        bottom=None
                    )
                )
                style_updates.append((cell, fmt_clear_borders))
                resolve_errors.append((month_year, idx, day_number))
                fixed_count += 1

        except Exception as e:
            print(f"[ERR] {cell}: Помилка при обробці — {e}")
            continue

# === Пакетне застосування стилів ===
if style_updates:
    try:
        format_cell_ranges(schedule_ws, style_updates)
    except Exception as e:
        print(f"[ERR] Не вдалося застосувати стилі пакетно — {e}")

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
