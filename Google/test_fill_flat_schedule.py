# === Опис функціоналу ===
# Цей скрипт виконує повну обробку табеля "Графік" у Google Sheets:
# 1. Завантажує вихідні дані з аркуша "Графік".
# 2. Формує пласку таблицю "фкт_ГрафікПлаский" з урахуванням фактичних початків/кінців змін і коментарів.
# 3. Зберігає внесені користувачем значення з попередньої версії таблиці.
# 4. Перевіряє зміни на перехльости — не лише в межах однієї дати, але й через північ, і **лише між сусідніми відсортованими змінами**.
# 5. Позначає конфлікти у колонці Error та перекреслює лише дані рядка (без перекреслення самої помилки).
# 6. Виводить хід обробки в консоль.

import os
import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# === Налаштування ===
SPREADSHEET_NAME = "zp_PetWealth"
SOURCE_SHEET = "Графік"
TARGET_SHEET = "фкт_ГрафікПлаский"
TOKEN_PATH = os.path.join(os.path.dirname(__file__), "token.json")

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

print("[INFO] Авторизація пройдена. Завантаження існуючих даних...")

# === Завантаження існуючих даних із пласкої таблиці ===
existing_data = tgt_ws.get_all_values()
header_row = [
    "Дата зміни", "IDX", "ПочатокЗміни", "КінецьЗміни", "Посада",
    "Відділення", "ТипЗміни", "Прізвище",
    "ФактПочаток", "ФактКінець", "Коментар", "Error"
]

# === Збереження користувацьких полів ===
user_fields = {}
for row in existing_data[1:]:
    if len(row) >= 12:
        key = tuple(row[:8])
        user_fields[key] = (row[8], row[9], row[10])

# === Очистка пласкої таблиці (залишаємо тільки хедер) ===
tgt_ws.clear()
tgt_ws.append_row(header_row, value_input_option="USER_ENTERED")
print("[OK] Таблиця очищена та заголовок додано.")

# === Завантаження даних із таблиці Графік ===
data = src_ws.get_all_values()
header = data[0]
flat_data = []

print("[INFO] Формування нових рядків...")
for row in data[1:]:
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

        key = (date_str, base[6], base[4], base[5], base[1], base[2], base[3], cell_value)
        fact_start, fact_end, comment = user_fields.get(key, ("", "", ""))
        flat_data.append([
            date_str, base[6], base[4], base[5], base[1], base[2], base[3], cell_value,
            fact_start, fact_end, comment, ""
        ])

if flat_data:
    tgt_ws.append_rows(flat_data, value_input_option="USER_ENTERED")
    print(f"[OK] Додано {len(flat_data)} актуальних рядків.")
else:
    print("[WARN] Даних для вставки немає у таблиці Графік.")

# === Перевірка на конфлікти ===
print("[INFO] Перевірка на конфлікти...")
existing_data = tgt_ws.get_all_values()
existing_data_rows = existing_data[1:]
error_rows = set()
flat_data_grouped = {}

for row in existing_data_rows:
    if len(row) >= 12:
        date, idx, start, end, posada, viddil, shift_type, surname = row[:8]
        fact_start = row[8].strip() if row[8].strip() else start
        fact_end = row[9].strip() if row[9].strip() else end

        try:
            dt_start = datetime.strptime(date + " " + fact_start, "%d.%m.%Y %H:%M")
            dt_end = datetime.strptime(date + " " + fact_end, "%d.%m.%Y %H:%M")
            if dt_end <= dt_start:
                dt_end += timedelta(days=1)
        except:
            continue

        flat_data_grouped.setdefault(surname, []).append((dt_start, dt_end, idx, posada, viddil, row))

conflicts = []
for surname, shifts in flat_data_grouped.items():
    sorted_shifts = sorted(shifts, key=lambda x: x[0])
    for i in range(len(sorted_shifts) - 1):
        start1, end1, idx1, pos1, vid1, row1 = sorted_shifts[i]
        start2, end2, idx2, pos2, vid2, row2 = sorted_shifts[i + 1]

        if start2 < end1:
            date1 = start1.strftime("%d.%m.%Y")
            date2 = start2.strftime("%d.%m.%Y")
            s1 = start1.strftime("%H:%M")
            e1 = end1.strftime("%H:%M")
            s2 = start2.strftime("%H:%M")
            e2 = end2.strftime("%H:%M")

            conflict_text = f"[WARN] Конфлікт: {surname} між {pos1} ({s1}-{e1}, {date1}) та {pos2} ({s2}-{e2}, {date2})"
            conflicts.append(conflict_text)

            row1_index = existing_data_rows.index(row1) + 2
            row2_index = existing_data_rows.index(row2) + 2

            row1[11] = f"Перетин з №{row2_index}: {date2}, {idx2}, {s2}-{e2}, {pos2}, {vid2}"
            row2[11] = f"Перетин з №{row1_index}: {date1}, {idx1}, {s1}-{e1}, {pos1}, {vid1}"

            error_rows.add(tuple(row1))
            error_rows.add(tuple(row2))

print(f"[INFO] Виявлено {len(conflicts)} конфлікт(ів)")

# === Формування колонки Error ===
error_column_values = []
for row in existing_data_rows:
    if tuple(row) in error_rows:
        error_column_values.append([row[11]])
    else:
        error_column_values.append([""])

print("[INFO] Оновлюємо колонку 'Error'...")
tgt_ws.update(error_column_values, range_name=f"L2:L{len(existing_data)}", value_input_option="USER_ENTERED")

# === Формування batchUpdate для перекреслення ===
print("[INFO] Формуємо перекреслення...")
requests = []
for idx, row in enumerate(existing_data_rows, start=2):
    strikethrough = bool(tuple(row) in error_rows)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": idx - 1,
                "endRowIndex": idx,
                "startColumnIndex": 0,
                "endColumnIndex": 11  # Перекреслює лише перші 11 колонок (без Error)
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"strikethrough": strikethrough}
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

if conflicts:
    print("\n[WARN] Знайдені конфлікти:")
    for conflict in conflicts:
        print(conflict)
else:
    print("\n[OK] Конфліктів не знайдено.")

print("\n[OK] Завершено: таблицю оновлено та перевірено на перехльости.")