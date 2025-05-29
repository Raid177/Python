# fill_flat_schedule.py

import os
import gspread
from datetime import datetime
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials

# === Авторизація через token.json ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = Credentials.from_authorized_user_file("token.json", SCOPES)
client = gspread.authorize(creds)

# === Налаштування ===
SPREADSHEET_NAME = "zp_PetWealth"
SHEET_SOURCE = "Графік"
SHEET_TARGET = "фкт_ГрафікПлаский"

KEY_FIELDS = ["Дата зміни", "Посада", "Відділення", "ТипЗміни", "ПочатокЗміни", "КінецьЗміни", "IDX"]

# === Отримуємо таблиці ===
ss = client.open(SPREADSHEET_NAME)
source_ws = ss.worksheet(SHEET_SOURCE)
target_ws = ss.worksheet(SHEET_TARGET)

source_data = source_ws.get_all_values()
target_data = target_ws.get_all_values()

target_header = target_data[0]
target_rows = target_data[1:]

# Побудова map по ключу
def build_key(row, header):
    return tuple(row[header.index(col)] for col in KEY_FIELDS)

existing_keys = {
    build_key(row, target_header): i + 2  # +2 бо get_all_values пропускає заголовок і рахунок з 1
    for i, row in enumerate(target_rows)
    if len(row) >= len(target_header)
}

# Обробка джерела
day_numbers = source_data[2][7:38]  # назви колонок H:AL — дні

new_rows = []
updated = 0
added = 0

for row in source_data[3:]:
    if len(row) < 38:
        continue
    try:
        month_str = row[0].strip()  # формат: 05.2025
        month, year = map(int, month_str.split('.'))

        idx = row[6]  # колонка G → IDX
        surname = row[7]  # колонка H → Прізвище
        role = row[1]
        dept = row[2]
        shift_type = row[3]
        shift_start = row[4]
        shift_end = row[5]

        for i, mark in enumerate(row[8:39]):  # колонки I:AM
            if mark.strip().lower() not in {"1", "д", "н"}:
                continue

            day = int(day_numbers[i])
            date_obj = datetime(year, month, day).strftime("%d.%m.%Y")

            flat_row = [
                date_obj, idx, shift_start, shift_end,
                role, dept, shift_type, surname,
                "", "", ""  # ФактПочаток, ФактКінець, Коментар
            ]

            key = build_key(flat_row, target_header)

            if key in existing_keys:
                row_index = existing_keys[key]
                target_ws.update_cell(row_index, target_header.index("Прізвище") + 1, surname)
                updated += 1
            else:
                new_rows.append(flat_row)
                added += 1

    except Exception as e:
        print(f"[⚠️] Помилка в рядку: {row} — {e}")
        continue

# Додавання нових рядків
if new_rows:
    target_ws.append_rows(new_rows, value_input_option="USER_ENTERED")

print(f"✅ Запис завершено: оновлено {updated}, додано {added}")
