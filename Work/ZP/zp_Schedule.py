import os
import pickle
import pandas as pd
import gspread
from mysql.connector import connect
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil.parser import parse

# === ШЛЯХИ === #
BASE_DIR = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Work/ZP"
ENV_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env"
CREDENTIALS_FILE = os.path.join(BASE_DIR, "client_secret.json")
TOKEN_FILE = os.path.join(BASE_DIR, "token.pickle")

# === НАЛАШТУВАННЯ === #
SHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_ГрафікПлаский"
TABLE_NAME = "zp_schedule"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# === Авторизація Google Sheets через OAuth === #
def get_gspread_client():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)
    return gspread.authorize(creds)

# === Основна логіка === #
def main():
    print("🔐 Авторизація Google Sheets...")
    client = get_gspread_client()

    print(f"📄 Завантаження аркуша: {SHEET_NAME}")
    worksheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    records = worksheet.get_all_records()
    df = pd.DataFrame(records)

    df.columns = df.columns.str.strip()
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    df.replace('', None, inplace=True)

    if df.empty:
        print("⚠️ Дані відсутні у таблиці.")
        return

    print("📂 Підключення до MySQL...")
    load_dotenv(dotenv_path=ENV_PATH)
    conn = connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE"),
        port=3306,
        charset="utf8mb4"
    )
    cursor = conn.cursor()

    added = 0
    updated = 0
    errors = 0

    for _, row in df.iterrows():
        try:
            uuid = row["UUID"].strip()
            start_str = row["ФактПочаток"] or row["ПочатокЗміни"]
            end_str = row["ФактКінець"] or row["КінецьЗміни"]

            start_dt = parse(start_str, dayfirst=True)
            end_dt = parse(end_str, dayfirst=True)

            if end_dt < start_dt:
                end_dt += timedelta(days=1)

            duration = end_dt - start_dt
            duration_seconds = int(duration.total_seconds())
            duration_hours = round(duration_seconds / 3600, 2)
            duration_str = str(duration)

            date_of_shift = parse(row["Дата зміни"], dayfirst=True).date()
            position = row["Посада"]
            department = row["Відділення"]
            shift_type = row["ТипЗміни"]
            surname = row["Прізвище"]
            comment = row["Коментар"]
            updated_at_sheet = parse(row["Оновлено"], dayfirst=True) if row["Оновлено"] else datetime.now()
            now = datetime.now()

            sql = f"""
                INSERT INTO {TABLE_NAME} (
                    UUID, ДатаЗміни, ПочатокЗміни, КінецьЗміни, Посада,
                    Відділення, ТипЗміни, Прізвище, Коментар, Оновлено,
                    ТривалістьД, ТривалістьГ, ТривалістьСек, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    ДатаЗміни=VALUES(ДатаЗміни),
                    ПочатокЗміни=VALUES(ПочатокЗміни),
                    КінецьЗміни=VALUES(КінецьЗміни),
                    Посада=VALUES(Посада),
                    Відділення=VALUES(Відділення),
                    ТипЗміни=VALUES(ТипЗміни),
                    Прізвище=VALUES(Прізвище),
                    Коментар=VALUES(Коментар),
                    Оновлено=VALUES(Оновлено),
                    ТривалістьД=VALUES(ТривалістьД),
                    ТривалістьГ=VALUES(ТривалістьГ),
                    ТривалістьСек=VALUES(ТривалістьСек),
                    updated_at=VALUES(updated_at)
            """

            cursor.execute(sql, (
                uuid, date_of_shift, start_dt, end_dt, position,
                department, shift_type, surname, comment, updated_at_sheet,
                duration_hours, duration_str, duration_seconds, now, now
            ))

            if cursor.rowcount == 1:
                added += 1
            elif cursor.rowcount == 2:
                updated += 1

        except Exception as e:
            print(f"❌ Помилка UUID {row.get('UUID', '---')}: {e}")
            errors += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\n✅ Додано: {added}, оновлено: {updated}, помилок: {errors}")

# === Запуск === #
if __name__ == "__main__":
    main()
