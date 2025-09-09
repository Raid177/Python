# Цей скрипт автоматично проставляє індекси IDX у таблиці "Графік" в Google Sheets для рядків, де колонка IDX порожня, але заповнена колонка "Посада". Він:
# Аналізує існуючі індекси за шаблоном префікс_номер.
# Формує нові унікальні індекси на основі скорочення Посади та порядкового номера.
# Оновлює їх у таблиці за допомогою пакетного запиту (batchUpdate) через Google Sheets API.
# Таким чином, кожному рядку з посадами присвоюється унікальний індекс для подальшої ідентифікації у базі даних або інших скриптах. [OK]

import os
import re
import json
import gspread
import requests
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# ==== Конфігурація ====
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly"
]
SPREADSHEET_NAME = "zp_PetWealth"
WORKSHEET_NAME = "Графік"
IDX_COLUMN_NAME = "IDX"
POSADA_COLUMN_NAME = "Посада"
# ======================

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
    return result.capitalize()

def auto_fill_idx():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    token_path = os.path.join(script_dir, "token.json")
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    client = gspread.authorize(creds)
    spreadsheet = client.open(SPREADSHEET_NAME)
    sheet = spreadsheet.worksheet(WORKSHEET_NAME)
    data = sheet.get_all_values()

    header = data[0]
    idx_col = header.index(IDX_COLUMN_NAME)
    pos_col = header.index(POSADA_COLUMN_NAME)

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

        if idx_col < len(row) and row[idx_col].strip():
            continue

        right_part = row[idx_col + 1:]
        if not any(cell.strip() for cell in right_part):
            continue

        posada = row[pos_col].strip()
        if not posada:
            continue

        prefix = get_abbreviation(posada)
        role_counter[prefix] = role_counter.get(prefix, 0) + 1
        new_idx = f"{prefix}_{role_counter[prefix]}"
        a1_notation = gspread.utils.rowcol_to_a1(i + 1, idx_col + 1)
        updates.append({
            "range": f"{WORKSHEET_NAME}!{a1_notation}",
            "majorDimension": "ROWS",
            "values": [[new_idx]]
        })

    if updates:
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet.id}/values:batchUpdate"
        headers = {
            "Authorization": f"Bearer {creds.token}",
            "Content-Type": "application/json"
        }
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": updates
        }
        response = requests.post(url, headers=headers, data=json.dumps(body))
        if response.ok:
            print(f"[OK] Оновлено {len(updates)} індексів")
        else:
            print(f"[ERROR] ПОМИЛКА batchUpdate: {response.status_code} — {response.text}")
    else:
        print("[WARN] Нічого не оновлено")

if __name__ == "__main__":
    auto_fill_idx()
