"""
[START] Скрипт завантаження та оновлення таблиці zp_worktime [START]

Функціонал:
- Підключається до Google Sheets (ліст "фкт_ГрафікПлаский").
- Зчитує таблицю графіка змін персоналу (дата, часи, посада тощо).
- Для кожного рядка:
    • Парсить дату та час зміни (враховуючи фактичні початок/кінець).
    • Обчислює тривалість зміни у форматі hh:mm та в годинах (десятковий формат).
    • Генерує унікальний ідентифікатор зміни shift_uuid (MD5-хеш від дати, idx та інших ключових полів).
    • Формує словник значень та вставляє/оновлює запис у таблиці zp_worktime.
- Видаляє зі zp_worktime записи, яких більше немає у графіку Google Sheets.
- Логує кількість доданих, оновлених та пропущених записів.
- Виводить інформаційні повідомлення про хід виконання скрипту.

Підключення до БД здійснюється через pymysql з використанням даних із .env файлу.

[OK] Скрипт можна запускати регулярно (крон або вручну) для актуалізації таблиці zp_worktime.
"""

# -*- coding: utf-8 -*-
"""
[START] Скрипт завантаження та оновлення таблиці zp_worktime (сервісний акаунт) [START]

Зміни:
- Авторизація до Google Sheets через service account JSON (без user-consent токена).
- Шлях до JSON береться з SA_JSON_PATH (env) або дефолтного шляху.

Порада: поділися таблицею з client_email із JSON сервісного акаунта.
"""

import os
import pymysql
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import hashlib

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as SA_Credentials

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
    load_dotenv(override=True)  # шукає .env у поточній папці

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"[ENV ERROR] Missing {name}. Check {ENV_PATH or '.env'}.")

# === Налаштування БД ===
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_POR", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

# === Google Sheets: Service Account JSON ===
SPREADSHEET_ID = os.getenv("ZP_SPREADSHEET_ID", "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw")
SHEET_NAME = os.getenv("ZP_SHEET_NAME", "фкт_ГрафікПлаский")
RANGE_NAME = f"{SHEET_NAME}!A1:Z"

SA_JSON_PATH = os.getenv("SA_JSON_PATH", "/root/Python/_Acces/zppetwealth-770254b6d8c1.json")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

if not os.path.exists(SA_JSON_PATH):
    raise SystemExit(f"[ENV ERROR] Service Account JSON not found: {SA_JSON_PATH}")

creds = SA_Credentials.from_service_account_file(SA_JSON_PATH, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

# === Утиліти часу ===
def parse_datetime(date_str, time_str):
    try:
        return datetime.strptime(f"{date_str} {time_str.strip()}", "%Y-%m-%d %H:%M")
    except Exception:
        return None

def calculate_duration(start_dt, end_dt):
    if not start_dt or not end_dt:
        return None, None, end_dt
    if end_dt < start_dt:
        end_dt += timedelta(days=1)
    delta = end_dt - start_dt
    hours = round(delta.total_seconds() / 3600, 1)
    text = str(delta)[:-3]  # hh:mm:ss -> hh:mm
    return text, hours, end_dt

# === Основна функція ===
def main():
    # 1) Тягнемо дані з Google Sheets
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME
    ).execute()
    data = result.get("values", [])
    if not data:
        print("[ERROR] Дані не знайдено у таблиці.")
        return

    header = data[0]
    rows = data[1:]
    # Вирівняти рядки під заголовок
    normalized_rows = [r + [None] * (len(header) - len(r)) for r in rows]
    df = pd.DataFrame(normalized_rows, columns=header)

    added, updated, skipped = 0, 0, 0
    current_keys = set()

    # 2) Підключення до БД
    conn = pymysql.connect(**DB_CONFIG)
    with conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                try:
                    raw_date = (row.get("Дата зміни") or "").strip()
                    idx = (row.get("IDX") or "").strip()
                    if not raw_date or not idx:
                        skipped += 1
                        continue

                    # 3) Парсинг дат/часів
                    date_shift = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")
                    base_start = (row.get("ПочатокЗміни") or "").strip()
                    base_end = (row.get("КінецьЗміни") or "").strip()
                    fact_start = (row.get("ФактПочаток") or "").strip()
                    fact_end = (row.get("ФактКінець") or "").strip()
                    comment = (row.get("Коментар") or "").strip()

                    use_fact = bool(fact_start and fact_end)
                    start_time = fact_start if use_fact else base_start
                    end_time = fact_end if use_fact else base_end

                    start_dt = parse_datetime(date_shift, start_time)
                    end_dt = parse_datetime(date_shift, end_time)
                    duration_text, duration_hours, end_dt = calculate_duration(start_dt, end_dt)

                    # 4) Формуємо запис
                    record = {
                        "date_shift": date_shift,
                        "idx": idx,
                        "time_start": start_dt.strftime("%Y-%m-%d %H:%M:%S") if start_dt else None,
                        "time_end": end_dt.strftime("%Y-%m-%d %H:%M:%S") if end_dt else None,
                        "position": row.get("Посада", "") or "",
                        "department": row.get("Відділення", "") or "",
                        "shift_type": row.get("ТипЗміни", "") or "",
                        "duration_text": duration_text,
                        "duration_hours": duration_hours,
                        "last_name": row.get("Прізвище", "") or "",
                        "is_corrected": "так" if use_fact else "ні",
                        "comment": comment if use_fact else "",
                    }

                    # 5) Генеруємо shift_uuid (стабільний ключ)
                    concat_str = f"{date_shift}_{idx}_{record['time_start']}_{record['time_end']}_{record['position']}_{record['department']}_{record['shift_type']}"
                    shift_uuid = hashlib.md5(concat_str.encode("utf-8")).hexdigest()
                    record["shift_uuid"] = shift_uuid

                    # 6) UPSERT
                    placeholders = ", ".join([f"`{k}`" for k in record])
                    values = ", ".join(["%s"] * len(record))
                    updates = ", ".join([f"`{k}`=VALUES(`{k}`)" for k in record if k not in ("date_shift", "idx")])

                    sql = f"""
                        INSERT INTO `zp_worktime` ({placeholders})
                        VALUES ({values})
                        ON DUPLICATE KEY UPDATE {updates}
                    """

                    cursor.execute(sql, list(record.values()))
                    current_keys.add((str(date_shift), idx))

                    # rowcount == 1 → insert; 2 → update (MySQL специфіка)
                    if cursor.rowcount == 1:
                        added += 1
                    else:
                        updated += 1

                except Exception as e:
                    print(f"[WARN] Помилка в IDX={row.get('IDX')}: {e}")
                    skipped += 1

            # 7) Видалення записів, яких більше немає в Google Sheets
            cursor.execute("SELECT `date_shift`, `idx` FROM `zp_worktime`")
            db_keys = {(str(r["date_shift"]), r["idx"]) for r in cursor.fetchall()}
            to_delete = db_keys - current_keys

            if to_delete:
                format_strings = ",".join(["(%s,%s)"] * len(to_delete))
                delete_sql = f"DELETE FROM `zp_worktime` WHERE (`date_shift`, `idx`) IN ({format_strings})"
                delete_values = [val for pair in to_delete for val in pair]
                cursor.execute(delete_sql, delete_values)
                print(f"🗑 Видалено: {len(to_delete)}")
            else:
                print("[INFO] Нічого не видалено.")

        conn.commit()

    print(f"[OK] Додано: {added}, Оновлено: {updated}, Пропущено: {skipped}")

if __name__ == "__main__":
    main()
