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

script_dir = os.path.dirname(os.path.abspath(__file__))
token_path = os.path.join(script_dir, "token.json")
creds = Credentials.from_authorized_user_file(token_path, SCOPES)
client = gspread.authorize(creds)

# === Налаштування ===
SPREADSHEET_NAME = "zp_PetWealth"
SHEET_SOURCE = "Графік"
SHEET_TARGET = "фкт_ГрафікПлаский"

KEY_FIELDS = ["Дата зміни", "Посада", "Відділення", "ТипЗміни", "ПочатокЗміни", "КінецьЗміни", "IDX"]

# === Отримати таблиці ===
ss = client.open(SPREADSHEET_NAME)
source_ws = ss.worksheet(SHEET_SOURCE)
target_ws = ss.worksheet(SHEET_TARGET)

source_data = source_ws.get_all_values()
target_data = target_ws.get_all_values()

target_header = target_data[0]
target_rows = target_data[1:]

# === Побудова ключа ===
def build_key(row, header):
    return tuple(row[header.index(col)] for col in KEY_FIELDS)

existing_keys = {
    build_key(row, target_header): i + 2  # +2 бо заголовок + 1-індексація
    for i, row in enumerate(target_rows)
    if len(row) >= len(target_header)
}

# === День місяця з H1:AL1 ===
day_numbers = source_data[0][7:38]  # колонки H–AL

# === Обробка рядків починаючи з 2-го (індекс 1) ===
new_rows = []
updated = 0
added = 0

for row in source_data[1:]:
    if len(row) < 38:
        continue
    try:
        month_str = row[0].strip()  # формат: 05.2025
        month, year = map(int, month_str.split('.'))

        posada = row[1]
        viddil = row[2]
        shift_type = row[3]
        shift_start = row[4]
        shift_end = row[5]
        idx = row[6]

        for i, mark in enumerate(row[7:38]):  # поля H–AL
            if not mark.strip():
                continue  # пропускаємо порожні клітинки

            day = int(day_numbers[i])
            date_obj = datetime(year, month, day).strftime("%d.%m.%Y")
            surname = mark.strip()

            flat_row = [
                date_obj, idx, shift_start, shift_end,
                posada, viddil, shift_type, surname,
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

# === Додавання нових рядків ===
if new_rows:
    target_ws.append_rows(new_rows, value_input_option="USER_ENTERED")

print(f"✅ Завершено. Оновлено: {updated} | Додано: {added}")
