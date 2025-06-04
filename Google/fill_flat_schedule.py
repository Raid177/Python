# === Опис функціоналу ===
# 1️⃣ Завантаження даних із Google Sheets («Графік» та «фкт_ГрафікПлаский»)
# 2️⃣ Збереження внесених користувачем значень (ФактПочаток, ФактКінець, Коментар) з пласкої таблиці
# 3️⃣ Очищення пласкої таблиці (залишаємо хедер)
# 4️⃣ Формування актуальної пласкої таблиці з урахуванням Графіка
# 5️⃣ Вставка збережених значень користувача (якщо є)
# 6️⃣ Перевірка на конфлікти змін (нахльости)
# 7️⃣ Заповнення колонки Error та застосування перекреслення рядків (batchUpdate)

import os
import json
import gspread
from datetime import datetime
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
        key = tuple(row[:8])  # Дата зміни, IDX, ПочатокЗміни, КінецьЗміни, Посада, Відділення, ТипЗміни, Прізвище
        user_fields[key] = (row[8], row[9], row[10])  # ФактПочаток, ФактКінець, Коментар

# === Очистка пласкої таблиці (залишаємо тільки хедер) ===
tgt_ws.clear()
tgt_ws.append_row(header_row, value_input_option="USER_ENTERED")
print("[OK] Таблиця очищена та заголовок додано.")

# === Завантаження даних із таблиці Графік ===
data = src_ws.get_all_values()
header = data[0]
flat_data = []

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

# === Додавання актуальних рядків у пласку таблицю ===
if flat_data:
    tgt_ws.append_rows(flat_data, value_input_option="USER_ENTERED")
    print(f"[OK] Додано {len(flat_data)} актуальних рядків.")
else:
    print("[WARN] Даних для вставки немає у таблиці Графік.")

# === Перевірка на конфлікти ===
existing_data = tgt_ws.get_all_values()
existing_data_rows = existing_data[1:]  # Пропускаємо хедер
error_rows = set()
flat_data_grouped = {}

for row in existing_data_rows:
    if len(row) >= 12:
        date, idx, start, end, posada, viddil, shift_type, surname = row[:8]
        fact_start = row[8].strip() if row[8].strip() else start
        fact_end = row[9].strip() if row[9].strip() else end

        key = (surname, date)
        if key not in flat_data_grouped:
            flat_data_grouped[key] = []
        flat_data_grouped[key].append((fact_start, fact_end, idx, posada, viddil, row))

conflicts = []
for key, shifts in flat_data_grouped.items():
    surname, date = key
    for i in range(len(shifts)):
        start1, end1, idx1, posada1, viddil1, row1 = shifts[i]
        try:
            s1 = int(start1.split(':')[0]) * 60 + int(start1.split(':')[1])
            e1 = int(end1.split(':')[0]) * 60 + int(end1.split(':')[1])
            if e1 <= s1:
                e1 += 24 * 60
            e1 -= 1
        except:
            continue

        for j in range(i + 1, len(shifts)):
            start2, end2, idx2, posada2, viddil2, row2 = shifts[j]
            try:
                s2 = int(start2.split(':')[0]) * 60 + int(start2.split(':')[1])
                e2 = int(end2.split(':')[0]) * 60 + int(end2.split(':')[1])
                if e2 <= s2:
                    e2 += 24 * 60
                e2 -= 1
            except:
                continue

            if (s1 < e2) and (s2 < e1):
                conflict_text = (
                    f"[WARN] Конфлікт: {surname} на {date} між {posada1} ({start1}-{end1}) та "
                    f"{posada2} ({start2}-{end2})"
                )
                conflicts.append(conflict_text)

                row1_index = existing_data_rows.index(row1) + 2  # +2 для коректного номера рядка
                row2_index = existing_data_rows.index(row2) + 2

                row1[11] = f"Перетин з рядком №{row2_index}: {date}, {idx2}, {start2}-{end2}, {posada2}, {viddil2}"
                row2[11] = f"Перетин з рядком №{row1_index}: {date}, {idx1}, {start1}-{end1}, {posada1}, {viddil1}"

                error_rows.add(tuple(row1))
                error_rows.add(tuple(row2))

# === Формування колонки Error ===
error_column_values = []
for row in existing_data_rows:
    if tuple(row) in error_rows:
        error_column_values.append([row[11]])
    else:
        error_column_values.append([""])

tgt_ws.update(error_column_values, range_name=f"L2:L{len(existing_data)}", value_input_option="USER_ENTERED")

# === Формування batchUpdate для перекреслення ===
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
                "endColumnIndex": 12
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"strikethrough": strikethrough}
                }
            },
            "fields": "userEnteredFormat.textFormat.strikethrough"
        }
    })

# === Виконуємо batchUpdate для закреслення ===
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
