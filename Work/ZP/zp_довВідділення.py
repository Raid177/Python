import os
import pickle
import pandas as pd
import gspread
from mysql.connector import connect
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv

# === ШЛЯХИ === #
BASE_DIR = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Work/ZP"
ENV_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env"
CREDENTIALS_FILE = os.path.join(BASE_DIR, "client_secret.json")
TOKEN_FILE = os.path.join(BASE_DIR, "token.pickle")

# === НАЛАШТУВАННЯ === #
SHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "дов_Відділення"
TABLE_NAME = "zp_довВідділення"
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

    # Очищення назв колонок і значень
    df.columns = df.columns.str.strip()
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    df.replace('', None, inplace=True)

    if df.empty:
        print("⚠️ Дані відсутні у таблиці.")
        return

    print("💾 Підключення до MySQL...")
    load_dotenv(dotenv_path=ENV_PATH)
    conn = connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE"),
        port=3306
    )
    cursor = conn.cursor()

    insert_sql = f"""
        INSERT INTO {TABLE_NAME} (`Відділення`)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE `Відділення` = VALUES(`Відділення`)
    """

    print(f"⬇️ Імпорт {len(df)} рядків у {TABLE_NAME}...")
    for _, row in df.iterrows():
        cursor.execute(insert_sql, (row['Відділення'],))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Імпорт завершено успішно.")

# === Запуск === #
if __name__ == "__main__":
    main()
