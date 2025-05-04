# zp_фктРівніСпівробітників.py
import os
import pickle
import pandas as pd
import gspread
from mysql.connector import connect
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv

BASE_DIR = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Work/ZP"
ENV_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env"
CREDENTIALS_FILE = os.path.join(BASE_DIR, "client_secret.json")
TOKEN_FILE = os.path.join(BASE_DIR, "token.pickle")

SHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_РівніСпівробітників"
TABLE_NAME = "zp_фктРівніСпівробітників"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

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

def main():
    print(f"🔐 Авторизація Google Sheets → {SHEET_NAME}")
    client = get_gspread_client()
    worksheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    raw_data = worksheet.get_all_values()

    headers = [h.strip() for h in raw_data[0]]
    data = raw_data[1:]

    df = pd.DataFrame(data, columns=headers)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace('', None)
    df = df.where(pd.notnull(df), None)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    print(f"📄 Зчитано {len(df)} рядків з таблиці {SHEET_NAME}")

    if df.empty:
        print("⚠️ Дані відсутні у таблиці.")
        return

    load_dotenv(dotenv_path=ENV_PATH)
    conn = connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE"),
        port=3306
    )
    cursor = conn.cursor()

    cursor.execute(f"DESCRIBE {TABLE_NAME}")
    db_columns = [row[0] for row in cursor.fetchall() if row[0] not in ("id", "created_at", "updated_at")]
    sheet_columns = df.columns.tolist()

    missing_in_sheet = set(db_columns) - set(sheet_columns)
    if missing_in_sheet:
        raise Exception(f"❌ У Google Sheet відсутні поля з БД: {missing_in_sheet}")

    new_columns = set(sheet_columns) - set(db_columns)
    for col in new_columns:
        print(f"➕ Додаю нове поле в БД: `{col}`")
        cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN `{col}` TEXT NULL")

    conn.commit()

    placeholders = ', '.join(['%s'] * len(sheet_columns))
    columns_str = ', '.join(f"`{col}`" for col in sheet_columns)
    updates_str = ', '.join(f"`{col}` = VALUES(`{col}`)" for col in sheet_columns)

    sql = f"""
        INSERT INTO {TABLE_NAME} ({columns_str})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {updates_str}
    """

    inserted, updated = 0, 0
    for _, row in df.iterrows():
        key_col = sheet_columns[0]
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE `{key_col}` = %s", (row[key_col],))
        exists = cursor.fetchone()[0]
        cursor.execute(sql, tuple(row[col] for col in sheet_columns))
        if exists:
            updated += 1
        else:
            inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Додано: {inserted}, оновлено: {updated}")

if __name__ == "__main__":
    main()
