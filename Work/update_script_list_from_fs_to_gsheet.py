# ============================================================================
# update_script_list_from_fs_to_gsheet.py
#
# 🔧 Що робить:
# 1. Шукає .py-файли у /root/Python (окрiм venv)
# 2. Додає новi в Google Sheet 'rules' з такими колонками:
#    [Папка, Скрипт, Інтервал запуску, Виконувати після, Паралельно, Коментар, Дубль?, Помилки]
# 3. Знімає жирність, центрування та гіперпосилання у перших двох колонках
# 4. Перевіряє:
#    - дублi (записує рядки в "Дубль?")
#    - некоректнi iнтервали ("❌ невідомий інтервал")
#    - неiснуючi залежностi ("❌ залежність не знайдено")
#    - циклiчнi залежностi ("❌ циклічна залежність")
# 5. 💾 Синхронізує дані в БД таблицю `cron_script_rules`, щоб працювати офлайн
#
# 📋 Формати інтервалу запуску (колонка "Інтервал запуску"):
#   - daily@04:00             → щодня о 4:00
#   - weekly@Mon 06:00        → щотижня в понеділок о 6:00
#   - monthly@1 03:00         → кожного 1-го числа місяця о 3:00
#   - hourly@4                → кожні 4 години
#   - every@2days             → кожні 2 дні
#   - Кілька інтервалів:      daily@04:00,weekly@Sun 06:00
#
# 📄 Таблиця Google Sheets: https://docs.google.com/spreadsheets/d/1bTvSME9yUbMJ6B6mlhWyOZwHGZwAXncWbdxsxnBfYbA
# ============================================================================


from pathlib import Path
import re
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv("/root/Python/.env")


# === Налаштування ===
TOKEN_PATH = "/root/Python/auth/token.json"
SPREADSHEET_ID = "1bTvSME9yUbMJ6B6mlhWyOZwHGZwAXncWbdxsxnBfYbA"
SHEET_NAME = "rules"
ROOT_DIR = Path("/root/Python")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

interval_patterns = [
    r"^$", r"^none$",
    r"^daily@\d{2}:\d{2}$",
    r"^weekly@\w{3} \d{2}:\d{2}$",
    r"^monthly@\d{1,2} \d{2}:\d{2}$",
    r"^hourly@\d{1,2}$",
    r"^every@\d+(days|weeks|months)$"
]

creds = Credentials.from_authorized_user_file(TOKEN_PATH, ["https://www.googleapis.com/auth/spreadsheets"])
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

# === Зчитування з Google Sheet ===
resp = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A2:H").execute()
rows = resp.get("values", [])

# === Очистка клітинок ===
cleaned = [[cell.strip() if isinstance(cell, str) else cell for cell in row] + [""] * (8 - len(row)) for row in rows]
sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=f"{SHEET_NAME}!A2:H",
    valueInputOption="USER_ENTERED",
    body={"values": cleaned}
).execute()
rows = cleaned

# === Виявлення нових скриптів ===
existing = {(r[0], r[1]): idx + 2 for idx, r in enumerate(rows) if len(r) >= 2}
all_scripts = []
for f in ROOT_DIR.rglob("*.py"):
    if "venv" in f.parts:
        continue
    folder = f.parent.as_posix().replace(str(ROOT_DIR), "").lstrip("/")
    all_scripts.append((folder, f.name))

all_seen = {}
for idx, row in enumerate(rows, start=2):
    if len(row) >= 2:
        key = (row[0], row[1])
        all_seen.setdefault(key, []).append(idx)

new = [s for s in all_scripts if s not in existing]
values_to_append = [[folder, script, "", "", "так", "", ", ".join(map(str, all_seen.get((folder, script), []))), ""] for folder, script in new]

if values_to_append:
    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A2",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values_to_append}
    ).execute()

    metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    sid = next(s["properties"]["sheetId"] for s in metadata["sheets"] if s["properties"]["title"] == SHEET_NAME)
    start = len(rows) + 1
    end = start + len(values_to_append)
    fmt = {
        "repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": start, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": 2},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": False}, "horizontalAlignment": "LEFT"}},
            "fields": "userEnteredFormat(textFormat,horizontalAlignment)"
        }
    }
    service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [fmt]}).execute()
    print(f"✅ Додано {len(values_to_append)} скриптів у таблицю.")
else:
    print("✅ Нових скриптів не знайдено.")

# === Перевірка залежностей та інтервалів ===
rows_normalized = [[cell.strip() if isinstance(cell, str) else "" for cell in row] + [""] * (8 - len(row)) for row in rows]
reverse_map = {(r[0], r[1]): i for i, r in enumerate(rows_normalized)}
deps = {}
errors = ["" for _ in rows_normalized]

for i, row in enumerate(rows_normalized):
    key = (row[0], row[1])
    after = row[3]
    if after:
        dep_keys = []
        for part in after.split(","):
            part = part.strip()
            if "/" in part:
                folder_dep, script_dep = map(str.strip, part.split("/", 1))
            elif "," in part:
                folder_dep, script_dep = map(str.strip, part.split(",", 1))
            else:
                folder_dep, script_dep = "", part
            dep_keys.append((folder_dep, script_dep))
        deps[key] = set(dep_keys)

for i, row in enumerate(rows_normalized):
    key = (row[0], row[1])
    for dep in deps.get(key, []):
        if dep not in reverse_map:
            errors[i] += "❌ залежність не знайдено; "
        elif key in deps.get(dep, set()):
            errors[i] += "❌ циклічна залежність; "

for i, row in enumerate(rows_normalized):
    interval = row[2]
    parts = [p.strip() for p in interval.split(",") if p.strip()]
    for part in parts:
        if not any(re.match(pat, part) for pat in interval_patterns):
            errors[i] += "❌ невідомий інтервал; "

# === Запис помилок ===
error_column = [[e.strip()] for e in errors]
sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=f"{SHEET_NAME}!H2",
    valueInputOption="USER_ENTERED",
    body={"values": error_column}
).execute()

# === Синхронізація з БД ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute("TRUNCATE TABLE cron_script_rules")
insert_sql = """
    INSERT INTO cron_script_rules (folder, script_name, run_interval, run_after, parallel, comment, duplicate_of, errors, synced_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
data = []
for i, row in enumerate(rows_normalized):
    folder = row[0]
    script = row[1]
    interval = row[2]
    after = row[3]
    parallel = row[4] or "так"
    comment = row[5]
    duplicate_of = row[6] if row[6] else None
    errs = error_column[i][0]
    synced_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data.append((folder, script, interval, after, parallel, comment, duplicate_of, errs, synced_at))

cursor.executemany(insert_sql, data)
conn.commit()
cursor.close()
conn.close()
print("✅ Дані збережено в таблицю cron_script_rules.")