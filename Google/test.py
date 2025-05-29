import os
import gspread
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from gspread_formatting import format_cell_range, CellFormat, textFormat

# Завантаження токена
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.metadata.readonly'
]
token_path = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Google/token.json"
creds = Credentials.from_authorized_user_file(token_path, SCOPES)
if creds.expired and creds.refresh_token:
    creds.refresh(Request())

# Авторизація
client = gspread.authorize(creds)
sheet = client.open("zp_PetWealth").worksheet("Test_Format")

# Клітинки для перекреслення
cells = ["A1", "B2", "C3", "C10"]

# Встановити перекреслення
for cell in cells:
    fmt = CellFormat(textFormat=textFormat(strikethrough=True))
    format_cell_range(sheet, cell, fmt)

print("✅ Перекреслення застосовано")
