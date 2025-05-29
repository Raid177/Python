"""
🛠 auto_fill_idx.py
- Підключається до Google Sheets
- Визначає останні використані номери індексів (Асс_4, Лкр_3, ...), включно з уже заповненими
- Проставляє нові індекси у форматі: Асс_5, Лкр_4, ...
- Пропускає рядки без прізвищ
- Не змінює вже існуючі індекси
"""

import os
import re
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly"
]

SPREADSHEET_NAME = "zp_PetWealth"
WORKSHEET_NAME = "Графік"
IDX_COLUMN_NAME = "IDX"
POSADA_COLUMN_NAME = "Посада"

def get_abbreviation(word):
    vowels = set("АЕЄИІЇОУЮЯ")
    result = word[0].upper()
    count = 0
    for char in word[1:]:
        char = char.lower()
        if char.upper() not in vowels:
            result += char
            count += 1
            if count == 2:
                break
    return result

def auto_fill_idx():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    token_path = os.path.join(script_dir, "token.json")
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    client = gspread.authorize(creds)
    sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
    data = sheet.get_all_values()

    header = data[0]
    idx_col = header.index(IDX_COLUMN_NAME)
    pos_col = header.index(POSADA_COLUMN_NAME)

    # 🧠 Спочатку проаналізуємо вже заповнені IDX
    role_counter = {}
    pattern = re.compile(r"^([А-ЯҐЄІЇа-яґєії]{3})_(\d+)$")

    for row in data[1:]:
        if idx_col < len(row):
            val = row[idx_col].strip()
            match = pattern.match(val)
            if match:
                prefix, number = match.groups()
                number = int(number)
                if prefix not in role_counter or number > role_counter[prefix]:
                    role_counter[prefix] = number

    updates = []

    for i in range(1, len(data)):
        row = data[i]

        # 🔒 Пропустити, якщо індекс вже є
        if idx_col < len(row) and row[idx_col].strip():
            continue

        # ⛔ Пропустити, якщо правіше немає жодних даних
        right_part = row[idx_col + 1:]
        if not any(cell.strip() for cell in right_part):
            continue

        posada = row[pos_col].strip()
        if not posada:
            continue

        prefix = get_abbreviation(posada)
        role_counter[prefix] = role_counter.get(prefix, 0) + 1
        new_idx = f"{prefix}_{role_counter[prefix]}"
        updates.append((i + 1, idx_col + 1, new_idx))

    for row, col, val in updates:
        sheet.update_cell(row, col, val)

    print(f"✅ Оновлено {len(updates)} індексів")

if __name__ == "__main__":
    auto_fill_idx()
