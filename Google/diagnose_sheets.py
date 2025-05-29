"""
🛠 diagnose_sheets.py
Цей скрипт:
- Підключається до Google Sheets через token.json
- Відкриває таблицю з назвою SPREADSHEET_NAME
- Виводить у консоль список усіх аркушів (вкладок) у цій таблиці
Використовується для перевірки правильної назви аркуша перед роботою основного скрипта (наприклад, auto_fill_idx.py).
"""



import os
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly"
]

SPREADSHEET_NAME = "zp_PetWealth"

def main():
    # Пошук токена поруч зі скриптом
    script_dir = os.path.dirname(os.path.abspath(__file__))
    token_path = os.path.join(script_dir, "token.json")
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    client = gspread.authorize(creds)
    spreadsheet = client.open(SPREADSHEET_NAME)

    print(f"📘 Відкрито таблицю: {SPREADSHEET_NAME}")
    print("📄 Доступні аркуші:")
    for ws in spreadsheet.worksheets():
        print(" -", ws.title)

if __name__ == "__main__":
    main()
