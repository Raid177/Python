import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Параметри
TOKEN_PATH = "/root/Python/auth/token.json"
SPREADSHEET_ID = "1bTvSME9yUbMJ6B6mlhWyOZwHGZwAXncWbdxsxnBfYbA"
SHEET_NAME = "rules"

# Авторизація
creds = Credentials.from_authorized_user_file(TOKEN_PATH, [
    "https://www.googleapis.com/auth/spreadsheets"
])
service = build("sheets", "v4", credentials=creds)

# Зчитування таблиці
result = service.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID,
    range=f"{SHEET_NAME}!A1:F"
).execute()
rows = result.get("values", [])

# У DataFrame
columns = rows[0]
data = rows[1:]
# Автодоповнення кожного рядка до 6 колонок
fixed_data = [row + [""] * (6 - len(row)) if len(row) < 6 else row[:6] for row in data]
df = pd.DataFrame(fixed_data, columns=columns)


# Вивід
print(df.to_markdown(index=False))
