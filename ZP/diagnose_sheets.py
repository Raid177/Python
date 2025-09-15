# staff_schedule_check_sa.py
# Отримує співробітників з Єнота → оновлює "дов_Співробітники" в Google Sheets →
# звіряє "Графік" і ставить/знімає закреслення для невалідних імен → лог у БД.
# Авторизація Google: СЕРВІСНИЙ АКАУНТ (JSON), без token.json.

import os
import re
import time
import json
import requests
import gspread
import pymysql
from datetime import datetime
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ========= ENV loader =========
ENV_PROFILE = os.getenv("ENV_PROFILE", "prod")  # dev | prod
ENV_PATHS = {
    "dev":  r"C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env",
    "prod": "/root/Python/_Acces/.env.prod",
}
ENV_PATH = os.getenv("ENV_PATH") or ENV_PATHS.get(ENV_PROFILE)
if ENV_PATH and os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)
else:
    load_dotenv(override=True)  # пробує .env в поточній папці

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"[ENV ERROR] Missing {name}. Check {ENV_PATH or '.env'}.")
    return v

# ========= Конфіг =========
# Google Sheets
SA_JSON_PATH     = "/root/Python/_Acces/zppetwealth-770254b6d8c1.json"  # <-- твій шлях
SPREADSHEET_ID_PW = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"       # ID таблиці 'zp_PetWealth'
STAFF_WS_NAME     = "дов_Співробітники"
SCHEDULE_WS_NAME  = "Графік"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    # дозволяє відкривати по title, якщо немає ID:
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]

# OData (Єнот)
ODATA_USER = require_env("ODATA_USER")
ODATA_PASSWORD = require_env("ODATA_PASSWORD")
ODATA_URL = (
    "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262/"
    "odata/standard.odata/Catalog_ФизическиеЛица"
    "?$select=Ref_Key,Code,Description&$filter=IsFolder eq false&$format=json"
)

# MySQL
DB_CFG = dict(
    host=require_env("DB_HOST"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=require_env("DB_USER"),
    password=require_env("DB_PASSWORD"),
    database=require_env("DB_DATABASE"),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=False,
)

# ========= Утиліти =========
def a1_to_gridrange(a1_notation: str, sheet_id: int) -> dict:
    m = re.match(r"([A-Z]+)(\d+)", a1_notation)
    if not m:
        raise ValueError(f"Invalid A1 notation: {a1_notation}")
    col, row = m.groups()
    col_idx = 0
    for ch in col:
        col_idx = col_idx * 26 + (ord(ch) - 64)
    col_idx -= 1
    return {
        "sheetId": sheet_id,
        "startRowIndex": int(row) - 1,
        "endRowIndex": int(row),
        "startColumnIndex": col_idx,
        "endColumnIndex": col_idx + 1,
    }

def mark_strikethrough(service, spreadsheet_id: str, sheet_id: int, cells: list[str], enable: bool = True):
    if not cells:
        return
    reqs = []
    for cell in cells:
        reqs.append({
            "repeatCell": {
                "range": a1_to_gridrange(cell, sheet_id),
                "cell": {"userEnteredFormat": {"textFormat": {"strikethrough": enable}}},
                "fields": "userEnteredFormat.textFormat.strikethrough",
            }
        })
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()

def get_spreadsheet(gc: gspread.Client):
    """Відкриває таблицю за ID (краще), або за назвою (потребує drive.metadata.readonly)."""
    if SPREADSHEET_ID_PW:
        return gc.open_by_key(SPREADSHEET_ID_PW)
    return gc.open(SPREADSHEET_NAME)

# ========= Основна логіка =========
def main():
    print("\n[LOG] Початок перевірки графіка —", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # --- Авторизація Google (service account) ---
    if not os.path.exists(SA_JSON_PATH):
        raise SystemExit(f"[ERROR] SA_JSON_PATH not found: {SA_JSON_PATH}")
    creds = Credentials.from_service_account_file(SA_JSON_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # --- Отримати співробітників з Єнота ---
    r = requests.get(ODATA_URL, auth=(ODATA_USER, ODATA_PASSWORD), timeout=60)
    r.raise_for_status()
    enote_staff = r.json().get("value", [])
    print(f"[LOG] Отримано {len(enote_staff)} співробітників з Єнота")

    # --- Відкрити таблицю та листи ---
    sh = get_spreadsheet(client)
    staff_ws = sh.worksheet(STAFF_WS_NAME)
    schedule_ws = sh.worksheet(SCHEDULE_WS_NAME)

    # --- Прочитати існуючий довідник для збереження ручних лінків "Графік" ---
    staff_records = staff_ws.get_all_records()  # [{'ПІБ':..., 'Графік':..., 'Code':..., 'Ref_Key':...}, ...]
    manual_links = {row.get('Ref_Key', ''): (row.get('Графік', '') or '').strip()
                    for row in staff_records if row.get('Ref_Key')}

    # --- Зібрати мапу з Єнота ---
    staff_map = {
        e['Ref_Key']: {
            'ПІБ': (e.get('Description') or '').strip(),
            'Code': e.get('Code') or '',
            'Графік': manual_links.get(e['Ref_Key'], '')
        }
        for e in enote_staff
    }

    # --- Підготувати нові рядки для Google Sheets ---
    new_rows = [[info['ПІБ'], info['Графік'], info['Code'], ref] for ref, info in staff_map.items()]

    # --- Оновити лист "дов_Співробітники" (очистити + записати) ---
    staff_ws.clear()
    header = ["ПІБ", "Графік", "Code", "Ref_Key"]
    staff_ws.update([header] + new_rows)
    print(f"[LOG] Оновлено дов_Співробітники — {len(new_rows)} рядків")

    # --- Зберегти в БД (повна перезаписка таблиці) ---
    conn = pymysql.connect(**DB_CFG)
    cur = conn.cursor()
    try:
        # створення таблиці — якщо потрібно
        cur.execute("""
            CREATE TABLE IF NOT EXISTS zp_довСпівробітники (
                `ПІБ` VARCHAR(255),
                `Графік` VARCHAR(255),
                `Code` VARCHAR(64),
                `Ref_Key` CHAR(36) PRIMARY KEY
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        cur.execute("DELETE FROM zp_довСпівробітники")
        db_rows = [(r[0], r[1], r[2], r[3]) for r in new_rows]
        if db_rows:
            cur.executemany("""
                INSERT INTO zp_довСпівробітники (`ПІБ`, `Графік`, `Code`, `Ref_Key`)
                VALUES (%s, %s, %s, %s)
            """, db_rows)
        conn.commit()
        print(f"[LOG] Дані записано в таблицю zp_довСпівробітники — {len(db_rows)} рядків")
    finally:
        cur.close()
        conn.close()

    # --- Перевірка графіка ---
    schedule_data = schedule_ws.get_all_values()
    valid_names = {row[1].strip() for row in new_rows if len(row) > 1 and row[1].strip()}
    sheet_id = schedule_ws.id
    spreadsheet_id = schedule_ws.spreadsheet.id

    print("[LOG] Починаємо перевірку клітинок з H2 по AL...")
    invalid_count = 0
    fixed_count = 0
    insert_errors = []
    resolve_errors = []
    strike_on, strike_off = [], []

    # H..AL => колонки 8..38 включно (A=1)
    for row_idx, row in enumerate(schedule_data[1:], start=2):
        month_year = (row[0] if len(row) > 0 else "").strip()
        idx = (row[6] if len(row) > 6 else "").strip()

        for col_idx in range(8, 39):
            name = (row[col_idx - 1] if len(row) >= col_idx else "").strip()
            if not name:
                continue

            cell = gspread.utils.rowcol_to_a1(row_idx, col_idx)
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

    # --- Застосування форматування (закреслення/зняття) ---
    mark_strikethrough(service, spreadsheet_id, sheet_id, strike_on, enable=True)
    mark_strikethrough(service, spreadsheet_id, sheet_id, strike_off, enable=False)

    # --- Лог помилок у БД ---
    conn = pymysql.connect(**DB_CFG)
    cur = conn.cursor()
    try:
        # Примітка: тут очікується, що таблиця zp_log_schedule_errors уже існує і має ключі
        if insert_errors:
            cur.executemany("""
                INSERT INTO zp_log_schedule_errors (month_year, idx, day_number, cell, value, comment)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    value = VALUES(value),
                    comment = VALUES(comment),
                    resolved_at = NULL
            """, insert_errors)

        if resolve_errors:
            cur.executemany("""
                UPDATE zp_log_schedule_errors
                SET resolved_at = NOW()
                WHERE month_year = %s AND idx = %s AND day_number = %s AND resolved_at IS NULL
            """, resolve_errors)

        conn.commit()
    finally:
        cur.close()
        conn.close()

    print(f"[LOG] Перевірка завершена. Некоректних клітинок: {invalid_count}, очищено форматів: {fixed_count}\n")

if __name__ == "__main__":
    main()
