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

# === Існуючі дані у пласкій таблиці ===
existing_data = tgt_ws.get_all_values()
existing_keys = set()
for row in existing_data[1:]:
    if len(row) >= 8:
        existing_keys.add(tuple(row[:8]))

# === Формування нових даних ===
final_data = []
batch_updates = []
deleted_rows = []
insert_counter = update_counter = cleared_counter = deleted = 0
updated_log = []

for row in flat_data:
    key = tuple(row[:8])
    if key in existing_keys:
        for i, ex_row in enumerate(existing_data[1:], start=2):
            if tuple(ex_row[:8]) == key:
                if len(ex_row) < 9 or ex_row[8] != row[7]:
                    batch_updates.append({'range': f"H{i}", 'values': [[row[7]]]})
                    if row[7] == "":
                        deleted_rows.append(i)
                        cleared_counter += 1
                    else:
                        update_counter += 1
                        updated_log.append(f"Оновлено H{i}: {row[7]}")
        continue
    final_data.append(row)
    insert_counter += 1

# === Видалення записів, які зникли з графіка ===
deleted_log = []
for i, ex_row in enumerate(existing_data[1:], start=2):
    if len(ex_row) >= 8:
        key = tuple(ex_row[:8])
        if key not in [tuple(row[:8]) for row in flat_data] and ex_row[7].strip():
            deleted_rows.append(i)
            cleared_counter += 1
            deleted_log.append(f"Видалено рядок {i}: {ex_row[:8]}")

# === Масове оновлення значень ===
if batch_updates:
    tgt_ws.batch_update(batch_updates)

# === Додавання нових рядків ===
if final_data:
    tgt_ws.append_rows(final_data, value_input_option="USER_ENTERED")

# === Масове видалення рядків через Google API ===
if deleted_rows:
    requests = []
    for i in sorted(deleted_rows, reverse=True):
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": i - 1,
                    "endIndex": i
                }
            }
        })
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()
    deleted = len(deleted_rows)

# === Логування ===
print(f"✅ Завершено. Додано: {insert_counter}, Оновлено: {update_counter}, Очищено: {cleared_counter}, Видалено: {deleted}, Пропущено помилкових: {skip_counter}")

if updated_log:
    print("\n🔄 Оновлені клітинки:")
    for line in updated_log:
        print(line)

if deleted_log:
    print("\n🗑️ Видалені рядки:")
    for line in deleted_log:
        print(line)

if skipped_log:
    print("\n⚠️ Пропущено через помилки:")
    for line in skipped_log:
        print(line)