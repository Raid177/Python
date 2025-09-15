# auto_fill_idx_sa.py
# Автоматично проставляє IDX у листі "Графік" (порожній IDX, але заповнена "Посада")
# Авторизація: сервісний акаунт (JSON), без token.json

import os
import re
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ==== Конфігурація ====
SA_JSON_PATH     = "/root/Python/_Acces/zppetwealth-770254b6d8c1.json"  # <-- твій шлях
SPREADSHEET_ID   = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"                        # <-- встав свій ID
WORKSHEET_NAME   = "Графік"
IDX_COLUMN_NAME  = "IDX"
POSADA_COLUMN_NAME = "Посада"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets"
]
# ======================

def get_abbreviation(word: str) -> str:
    vowels = set("АЕЄИІЇОУЮЯаеєиіїоюя")
    word = (word or "").strip()
    if not word:
        return ""
    result = word[0].upper()
    count = 0
    for ch in word[1:]:
        if ch not in vowels:
            result += ch.lower()
            count += 1
            if count == 2:
                break
    return result.capitalize()

def main():
    # --- Авторизація через сервісний акаунт ---
    if not os.path.exists(SA_JSON_PATH):
        raise FileNotFoundError(f"SA_JSON_PATH not found: {SA_JSON_PATH}")
    creds = Credentials.from_service_account_file(SA_JSON_PATH, scopes=SCOPES)

    # gspread — для читання і утиліти A1
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)  # уникаємо Drive API, працюємо по ID
    ws = sh.worksheet(WORKSHEET_NAME)

    # --- Читаємо всі значення листа ---
    data = ws.get_all_values()
    if not data:
        print("[WARN] Лист порожній")
        return

    header = data[0]
    try:
        idx_col = header.index(IDX_COLUMN_NAME) + 1  # 1-based для A1
        pos_col = header.index(POSADA_COLUMN_NAME) + 1
    except ValueError as e:
        raise RuntimeError(f"Не знайдено колонки '{IDX_COLUMN_NAME}' або '{POSADA_COLUMN_NAME}' у заголовку") from e

    # --- Збираємо вже існуючі префікси/номери ---
    role_counter = {}
    pattern = re.compile(r"^([А-ЯҐЄІЇа-яґєії]{3})_(\d+)$")
    for r in range(2, len(data) + 1):  # починаємо з 2-го рядка (після заголовка)
        row = data[r - 1]
        if idx_col <= len(row):
            val = (row[idx_col - 1] or "").strip()
            m = pattern.match(val)
            if m:
                prefix, num = m.groups()
                num = int(num)
                role_counter[prefix] = max(role_counter.get(prefix, 0), num)

    # --- Формуємо оновлення ---
    updates = []
    for r in range(2, len(data) + 1):
        row = data[r - 1]
        # пропустити, якщо IDX вже є
        if idx_col <= len(row) and (row[idx_col - 1] or "").strip():
            continue

        # пропустити, якщо "Посада" порожня
        posada = (row[pos_col - 1] if pos_col <= len(row) else "").strip()
        if not posada:
            continue

        prefix = get_abbreviation(posada)
        if not prefix or len(prefix) < 3:
            # якщо з якоїсь причини префікс коротший 3, доповнимо X
            prefix = (prefix + "XXX")[:3]

        role_counter[prefix] = role_counter.get(prefix, 0) + 1
        new_idx = f"{prefix}_{role_counter[prefix]}"

        a1 = gspread.utils.rowcol_to_a1(r, idx_col)
        # назву листа завжди беремо в апострофи (на випадок пробілів/кирилиці)
        updates.append({
            "range": f"'{WORKSHEET_NAME}'!{a1}",
            "majorDimension": "ROWS",
            "values": [[new_idx]]
        })

    if not updates:
        print("[WARN] Нічого оновлювати — всі IDX вже заповнені або немає рядків з 'Посада'.")
        return

    # --- Відправляємо пакетне оновлення через Sheets API ---
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    body = {"valueInputOption": "USER_ENTERED", "data": updates}
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body=body
    ).execute()

    print(f"[OK] Оновлено {len(updates)} індексів")

if __name__ == "__main__":
    main()
