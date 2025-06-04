import os
import requests
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# === Налаштування ===

# Абсолютний шлях до .env файлу
ENV_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env"
load_dotenv(ENV_PATH)

# OData (Єнот)
ODATA_URL_BASE = os.getenv("ODATA_URL")
ODATA_TABLE = "Catalog_АналитикаПоЗарплате"
ODATA_URL = f"{ODATA_URL_BASE}{ODATA_TABLE}?$format=json&$filter=IsFolder eq false&$select=Description,Ref_Key,Code"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

# Google Sheets
SPREADSHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "дов_АнЗП"
RANGE_NAME = f"{SHEET_NAME}!A1:Z"
TOKEN_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Google/token.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly"
]

# === Отримання даних з OData ===
try:
    response = requests.get(ODATA_URL, auth=(ODATA_USER, ODATA_PASSWORD))
    response.raise_for_status()
    records = response.json().get('value', [])
    print(f"[OK] Отримано {len(records)} записів з Єноту")
except Exception as e:
    print(f"[ERROR] Помилка під час отримання даних з OData: {e}")
    exit(1)

# === Формування даних для Google Sheets ===
headers = ['Description', 'Ref_Key', 'Code']
values = [headers] + [[r.get('Description', ''), r.get('Ref_Key', ''), r.get('Code', '')] for r in records]

# === Авторизація Google Sheets ===
try:
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Очищення старих даних
    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        body={}
    ).execute()
    print(f"[DELETE] Дані на листі '{SHEET_NAME}' очищено.")

    # Запис нових даних
    body = {'values': values}
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption='RAW',
        body=body
    ).execute()
    print(f"[OK] Дані успішно оновлено на листі '{SHEET_NAME}'.")
except Exception as e:
    print(f"[ERROR] Помилка під час оновлення Google Sheets: {e}")
    exit(1)
