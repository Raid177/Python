# === Опис функціоналу ===
# 1️⃣ Підключення до Google Sheets і MySQL, авторизація через token.json та .env.
# 2️⃣ Завантаження даних із аркуша "Графік" та перевірка наявності помилкових клітинок у таблиці zp_log_schedule_errors.
# 3️⃣ Формування плоскої таблиці (flat_data) для подальшої обробки.
# 4️⃣ Перевірка на конфлікти змін (перетин змін співробітників у межах однієї дати).
#     ➡️ Якщо є конфлікт: додається рядок у колонку L, рядок закреслюється.
#     ➡️ Якщо конфлікту немає: знімається закреслення і очищається колонка L.
# 5️⃣ Логування знайдених конфліктів у консоль.
# 6️⃣ Масове оновлення форматування та даних у таблиці "фкт_ГрафікПлаский" через Google API.
# 7️⃣ Скрипт завершується повідомленням про кількість знайдених та оброблених конфліктів.


import os
import re
import json
from datetime import datetime
import pymysql
import gspread
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from gspread_formatting import get_effective_format
from gspread.utils import rowcol_to_a1
from dotenv import load_dotenv

# === Налаштування ===
SPREADSHEET_NAME = "zp_PetWealth"
SOURCE_SHEET = "Графік"
TARGET_SHEET = "фкт_ГрафікПлаский"
TOKEN_PATH = os.path.join(os.path.dirname(__file__), "token.json")
ENV_PATH = os.path.join(os.path.dirname(__file__), "../.env")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly"
]

# === Авторизація Google Sheets ===
with open(TOKEN_PATH, "r") as token_file:
    token_info = json.load(token_file)
    creds = Credentials.from_authorized_user_info(token_info, SCOPES)

client = gspread.authorize(creds)
spreadsheet = client.open(SPREADSHEET_NAME)
src_ws = spreadsheet.worksheet(SOURCE_SHEET)
tgt_ws = spreadsheet.worksheet(TARGET_SHEET)

# === Авторизація Google Sheets API (для batchUpdate) ===
service = build('sheets', 'v4', credentials=creds)
spreadsheet_id = spreadsheet.id
sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
sheet_id = next(s['properties']['sheetId'] for s in sheet_metadata['sheets'] if s['properties']['title'] == TARGET_SHEET)

# === Авторизація до MySQL ===
load_dotenv(ENV_PATH)
db = pymysql.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_DATABASE"),
    charset='utf8mb4'
)
cursor = db.cursor()

# === Перевірка таблиці помилок ===
cursor.execute("""
    SELECT `month_year`, `day_number`, `idx`, `value`
    FROM zp_log_schedule_errors
    WHERE `resolved_at` IS NULL
""")
error_cells = set((row[0], str(row[1]), row[2], row[3]) for row in cursor.fetchall())

# === Отримання даних з графіка ===
data = src_ws.get_all_values()
header = data[0]

# === Обробка графіка ===
flat_data = []
skip_counter = 0
skipped_log = []
for i, row in enumerate(data[1:], start=2):
    base = row[:7]
    try:
        month_year_str = base[0].strip()
        month, year = map(int, month_year_str.split("."))
    except:
        continue

    for col in range(7, len(row)):
        cell_value = row[col].strip()
        try:
            day = int(header[col])
        except:
            continue

        try:
            date_str = datetime(year, month, day).strftime("%d.%m.%Y")
        except:
            continue

        if not cell_value:
            continue

        if (f"{month:02d}.{year}", str(day), base[6], cell_value) in error_cells:
            skip_counter += 1
            skipped_log.append(f"Пропущено: {month:02d}.{year} {day} {base[6]} {cell_value}")
            continue

        flat_data.append([
            date_str, base[6], base[4], base[5], base[1], base[2], base[3], cell_value
        ])

# === Перевірка на конфлікти ===
conflicts = []
error_rows = set()
flat_data_grouped = {}
for row in flat_data:
    date, idx, start, end, posada, viddil, shift_type, surname = row
    key = (surname, date)
    if key not in flat_data_grouped:
        flat_data_grouped[key] = []
    flat_data_grouped[key].append((start, end, idx, posada, row))

for key, shifts in flat_data_grouped.items():
    surname, date = key
    for i in range(len(shifts)):
        start1, end1, idx1, posada1, row1 = shifts[i]
        try:
            s1 = int(start1.split(':')[0]) * 60 + int(start1.split(':')[1])
            e1 = int(end1.split(':')[0]) * 60 + int(end1.split(':')[1])
            if e1 <= s1:
                e1 += 24 * 60
        except:
            continue
        for j in range(i + 1, len(shifts)):
            start2, end2, idx2, posada2, row2 = shifts[j]
            try:
                s2 = int(start2.split(':')[0]) * 60 + int(start2.split(':')[1])
                e2 = int(end2.split(':')[0]) * 60 + int(end2.split(':')[1])
                if e2 <= s2:
                    e2 += 24 * 60
            except:
                continue
            if (s1 < e2) and (s2 < e1):
                conflict_text = f"⚠️ Конфлікт: {surname} на {date} між {posada1} ({start1}-{end1}) та {posada2} ({start2}-{end2})"
                conflicts.append(conflict_text)
                error_rows.add(tuple(row1))
                error_rows.add(tuple(row2))

if conflicts:
    print("\n⚠️ Знайдені конфлікти:")
    for conflict in conflicts:
        print(conflict)

# === Існуючі дані у пласкій таблиці ===
existing_data = tgt_ws.get_all_values()
batch_updates = []

for i, ex_row in enumerate(existing_data[1:], start=2):
    if len(ex_row) >= 8:
        row_key = tuple(ex_row[:8])
        if row_key in error_rows:
            batch_updates.append({"range": f"L{i}", "values": [["Конфлікт змін"]]})
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": i - 1,
                                    "endRowIndex": i,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": 8
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"strikethrough": True}
                                    }
                                },
                                "fields": "userEnteredFormat.textFormat.strikethrough"
                            }
                        }
                    ]
                }
            )
        else:
            batch_updates.append({"range": f"L{i}", "values": [[""]]})
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": i - 1,
                                    "endRowIndex": i,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": 8
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"strikethrough": False}
                                    }
                                },
                                "fields": "userEnteredFormat.textFormat.strikethrough"
                            }
                        }
                    ]
                }
            )

if batch_updates:
    tgt_ws.batch_update(batch_updates)

print("\n✅ Перевірка конфліктів завершена")
