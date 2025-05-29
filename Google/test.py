import os
import gspread
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from gspread_formatting import *

# Завантаження змінних середовища
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

# Авторизація через token.json
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json")
creds = Credentials.from_authorized_user_file(token_path, SCOPES)
if creds.expired and creds.refresh_token:
    creds.refresh(Request())

client = gspread.authorize(creds)

# Відкриваємо тестовий аркуш
sheet = client.open("zp_PetWealth").worksheet("Графік")

# Тестові координати клітинки
cell = "B2"

# Наносимо дуже товстий червоний бордюр
fmt = cellFormat(
    borders=Borders(
        left=Border("SOLID_THICK", Color(1, 0, 0)),
        right=Border("SOLID_THICK", Color(1, 0, 0)),
        top=Border("SOLID_THICK", Color(1, 0, 0)),
        bottom=Border("SOLID_THICK", Color(1, 0, 0))
    )
)
format_cell_range(sheet, cell, fmt)
print(f"[TEST] Дуже товстий червоний бордюр встановлено в {cell}")

# Знімаємо лише бордюр (залишаємо текст)
fmt_clear_borders = cellFormat(
    borders=Borders(
        left=Border("NONE"),
        right=Border("NONE"),
        top=Border("NONE"),
        bottom=Border("NONE")
    )
)
format_cell_range(sheet, cell, fmt_clear_borders)
print(f"[TEST] Бордюр очищено в {cell}")
