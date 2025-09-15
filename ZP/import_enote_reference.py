# sync_anzp_sa.py
# Завантажує довідник із OData та записує в Google Sheets (сервісний акаунт)

import os
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# === Конфіг середовища =======================================================
ENV_PROFILE = os.getenv("ENV_PROFILE", "prod")  # dev | prod
ENV_PATHS = {
    "dev":  r"C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env",
    "prod": "/root/Python/_Acces/.env.prod",  # за потреби зміни на свій шлях
}
# завантажуємо .env, якщо існує (нічого страшного, якщо файлу немає)
load_dotenv(ENV_PATHS.get(ENV_PROFILE, ""))

# === OData (Єнот) ============================================================
ODATA_URL_BASE = os.getenv("ODATA_URL")
ODATA_TABLE = "Catalog_АналитикаПоЗарплате"
ODATA_URL = f"{ODATA_URL_BASE}{ODATA_TABLE}?$format=json&$filter=IsFolder eq false&$select=Description,Ref_Key,Code"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

# === Google Sheets ===========================================================
# Рекомендую тримати ID у .env як SPREADSHEET_ID_AnZP
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID_AnZP", "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw")
SHEET_NAME = "дов_АнЗП"
RANGE_NAME = f"'{SHEET_NAME}'!A1:Z"

# Шлях до JSON сервісного акаунта
SA_JSON_PATH = os.getenv("SA_JSON_PATH", "/root/Python/_Acces/zppetwealth-770254b6d8c1.json")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def main():
    # === Отримання даних з OData ===
    try:
        resp = requests.get(ODATA_URL, auth=(ODATA_USER, ODATA_PASSWORD), timeout=60)
        resp.raise_for_status()
        records = resp.json().get("value", [])
        print(f"[OK] Отримано {len(records)} записів з Єноту")
    except Exception as e:
        print(f"[ERROR] Помилка під час отримання даних з OData: {e}")
        raise SystemExit(1)

    # === Формування даних для Google Sheets ===
    headers = ["Description", "Ref_Key", "Code"]
    values = [headers] + [[r.get("Description", ""), r.get("Ref_Key", ""), r.get("Code", "")] for r in records]

    # === Авторизація Google Sheets через сервісний акаунт ===
    try:
        if not os.path.exists(SA_JSON_PATH):
            raise FileNotFoundError(f"Не знайдено SA_JSON_PATH: {SA_JSON_PATH}")
        creds = Credentials.from_service_account_file(SA_JSON_PATH, scopes=SCOPES)
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        sheet = service.spreadsheets()

        # Очищаємо діапазон
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            body={}
        ).execute()
        print(f"[DELETE] Дані на листі '{SHEET_NAME}' очищено.")

        # Записуємо нові значення
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print(f"[OK] Дані успішно оновлено на листі '{SHEET_NAME}'.")
    except Exception as e:
        print(f"[ERROR] Помилка під час оновлення Google Sheets: {e}")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
