import os
import pymysql
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# === Завантаження змінних із .env ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

# === Дані Google Sheets ===
SPREADSHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_ГрафікПлаский"
RANGE_NAME = f"{SHEET_NAME}!A1:Z"
TOKEN_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Google/token.json"

# === Авторизація через token.json ===
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly"
]
creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
service = build("sheets", "v4", credentials=creds)

# === Допоміжні функції ===
def parse_datetime(date_str, time_str):
    try:
        return datetime.strptime(f"{date_str} {time_str.strip()}", "%Y-%m-%d %H:%M")
    except:
        return None

def calculate_duration(start_dt, end_dt):
    if not start_dt or not end_dt:
        return None, None
    if end_dt < start_dt:
        end_dt += timedelta(days=1)
    delta = end_dt - start_dt
    hours = round(delta.total_seconds() / 3600, 1)
    text = str(delta)[:-3]  # формат hh:mm
    return text, hours

# === Основна логіка ===
def main():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME
    ).execute()
    data = result.get("values", [])
    if not data:
        print("❌ Дані не знайдено.")
        return

    header = data[0]
    rows = data[1:]
    normalized_rows = [r + [None] * (len(header) - len(r)) for r in rows]
    df = pd.DataFrame(normalized_rows, columns=header)

    added, updated, skipped = 0, 0, 0
    conn = pymysql.connect(**DB_CONFIG)

    with conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                try:
                    raw_date = (row.get("Дата зміни") or "").strip()
                    try:
                        date_shift = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")
                    except:
                        skipped += 1
                        continue

                    idx = row.get("IDX")
                    if not date_shift or not idx:
                        skipped += 1
                        continue

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
                    duration_text, duration_hours = calculate_duration(start_dt, end_dt)

                    record = {
                        "date_shift": date_shift,
                        "idx": idx,
                        "time_start": start_time,
                        "time_end": end_time,
                        "position": row.get("Посада", ""),
                        "department": row.get("Відділення", ""),
                        "shift_type": row.get("ТипЗміни", ""),
                        "duration_text": duration_text,
                        "duration_hours": duration_hours,
                        "last_name": row.get("Прізвище", ""),
                        "is_corrected": "так" if use_fact else "ні",
                        "comment": comment if use_fact else "",
                    }

                    placeholders = ", ".join([f"`{k}`" for k in record])
                    values = ", ".join(["%s"] * len(record))
                    updates = ", ".join([f"`{k}`=VALUES(`{k}`)" for k in record if k not in ("date_shift", "idx")])

                    sql = f"""
                        INSERT INTO zp_worktime ({placeholders})
                        VALUES ({values})
                        ON DUPLICATE KEY UPDATE {updates}
                    """
                    # print(f"{date_shift=}, {idx=}")

                    cursor.execute(sql, list(record.values()))
                    if cursor.rowcount == 1:
                        added += 1
                    else:
                        updated += 1

                except Exception as e:
                    print(f"⚠️ Помилка в рядку IDX={row.get('IDX')}: {e}")
                    skipped += 1

        conn.commit()

    print(f"✅ Додано: {added}, Оновлено: {updated}, Пропущено: {skipped}")

if __name__ == "__main__":
    main()
