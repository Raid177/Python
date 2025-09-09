"""
Скрипт для підтягування рівнів співробітників та розрахунку умов оплати у таблиці zp_worktime.

🔹 Скрипт має два основних блоки:
1️⃣ Синхронізація рівнів співробітників (таблиця zp_фктРівніСпівробітників):
   - Зчитує дані з Google Sheets.
   - Виконує триммування текстових полів і підміняє NULL на '' для уникнення дублів.
   - Перевіряє дублікати (Прізвище, ДатаПочатку, Посада, Відділення, Рівень):
       • Якщо є дублікати — зупиняє виконання та виводить їх у консоль.
   - Автоматично розставляє ДатаЗакінчення для попереднього рівня співробітника (якщо є перехід на новий).
   - Оновлює Google Sheets із датами закінчення.
   - Завантажує дані у таблицю zp_фктРівніСпівробітників в MySQL:
       • INSERT ... ON DUPLICATE KEY UPDATE (оновлює або додає рядок).
   - Логування дій у консоль:
       • Попередження про дублікати.
       • Нові та оновлені записи.
       • Закриття рівнів.

2️⃣ Розрахунок збігів у таблиці zp_worktime:
   - Підтягує рівень співробітника у zp_worktime з таблиці zp_фктРівніСпівробітників.
   - Розраховує Rule_ID, Matches та Score для кожного запису, використовуючи таблицю правил zp_фктУмовиОплати.
   - Записує колізії у поле Colision (якщо знайдено кілька рівнів або кілька Rule_ID з однаковою вагою).
   - Записує інші помилки у поле ErrorLog.
   - Оновлює поля:
       • Matches
       • Score
       • СтавкаЗміна
       • СтавкаГодина
       • Rule_ID
       • ErrorLog
       • Colision
   - Логування результатів у консоль.

[OK] Важливо:
- Скрипт виконує обидва блоки по черзі: спочатку синхронізація рівнів, потім обробка zp_worktime.
- Скрипт зупиняє роботу, якщо знайдено дублікати в Google Sheets.
"""


"""
Скрипт для підтягування рівнів співробітників та розрахунку умов оплати у таблиці zp_worktime.
"""

import os
import sys
import pymysql
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

# === Завантаження .env ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST_Serv"),
    "port": int(os.getenv("DB_PORT_Serv", 3306)),  # ✅ дефолт — 3306
    "user": os.getenv("DB_USER_Serv"),
    "password": os.getenv("DB_PASSWORD_Serv"),
    "database": os.getenv("DB_DATABASE_Serv"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}


SPREADSHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_РівніСпівробітників"
RANGE_NAME = f"{SHEET_NAME}!A1:Z"
TOKEN_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Google/token.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def sync_employee_levels():
    log_message("[SYNC] Початок синхронізації рівнів співробітників...")
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get("values", [])

    if not values:
        log_message("[ERROR] Дані не знайдено в Google Sheets!")
        return

    header = values[0]
    data = values[1:]
    df = pd.DataFrame(data, columns=header)
    df["ДатаПочатку"] = pd.to_datetime(df["ДатаПочатку"], dayfirst=True, errors="coerce")
    df["ДатаЗакінчення"] = pd.to_datetime(df["ДатаЗакінчення"], dayfirst=True, errors="coerce")

    for col in ["Прізвище", "Посада", "Відділення", "Рівень"]:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else "")

    duplicate_mask = df.duplicated(subset=["Прізвище", "ДатаПочатку", "Посада", "Відділення", "Рівень"], keep=False)
    if duplicate_mask.any():
        log_message("[WARN] В Google Sheets знайдено дублікати, виправте їх перед завантаженням.")
        for _, row in df[duplicate_mask].iterrows():
            log_message(f"  - {row['Прізвище']} | {row['ДатаПочатку']} | {row['Посада']} | {row['Відділення']} | {row['Рівень']}")
        sys.exit(1)

    df.sort_values(by=["Прізвище", "Посада", "Відділення", "ДатаПочатку"], inplace=True)

    prev_row = None
    for idx, row in df.iterrows():
        if pd.isnull(row["ДатаПочатку"]):
            continue
        key = (row["Прізвище"], row["Посада"], row["Відділення"])
        if prev_row is not None:
            prev_key = (prev_row["Прізвище"], prev_row["Посада"], prev_row["Відділення"])
            if key == prev_key:
                if pd.isnull(prev_row["ДатаЗакінчення"]) and row["ДатаПочатку"] > prev_row["ДатаПочатку"]:
                    df.at[prev_row.name, "ДатаЗакінчення"] = row["ДатаПочатку"] - timedelta(days=1)
                    log_message(f"[SYNC] Закрито рівень: {prev_row['Прізвище']} {prev_row['Посада']} {prev_row['Відділення']} → {df.at[prev_row.name, 'ДатаЗакінчення'].date()}")
        prev_row = row if pd.isnull(row["ДатаЗакінчення"]) else None

    updated_values = [header]
    for _, row in df.iterrows():
        
        row_out = []
        for col in header:
            val = row.get(col, "")
            if isinstance(val, pd.Timestamp):
                if pd.isnull(val):
                    row_out.append("")
                else:
                    row_out.append(val.strftime("%d.%m.%Y"))
            elif pd.isnull(val):
                row_out.append("")
            else:
                row_out.append(str(val))

        
        updated_values.append(row_out)

    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption="RAW",
        body={"values": updated_values}
    ).execute()
    log_message("[OK] Google Sheets оновлено!")

    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    inserted, updated = 0, 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO zp_фктРівніСпівробітників (`Прізвище`, `ДатаПочатку`, `ДатаЗакінчення`, `Посада`, `Відділення`, `Рівень`)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `ДатаЗакінчення` = VALUES(`ДатаЗакінчення`),
                `Рівень` = VALUES(`Рівень`)
        """, (
            row["Прізвище"],
            row["ДатаПочатку"].strftime("%Y-%m-%d") if pd.notnull(row["ДатаПочатку"]) else None,
            row["ДатаЗакінчення"].strftime("%Y-%m-%d") if pd.notnull(row["ДатаЗакінчення"]) else None,
            row["Посада"], row["Відділення"], row["Рівень"]
        ))
        if cursor.rowcount == 1:
            inserted += 1
        elif cursor.rowcount == 2:
            updated += 1

    conn.commit()
    cursor.close()
    conn.close()
    log_message(f"[OK] Завантажено: {inserted} додано, {updated} оновлено.")

def calculate_worktime_matches():
    log_message("[INFO] Початок розрахунку збігів у zp_worktime...")

    weights = {
        'position': 50, 'last_name': 40,
        'department': 30, 'level': 20, 'shift_type': 10
    }
    field_mapping = {
        'position': 'Посада', 'last_name': 'Прізвище',
        'department': 'Відділення', 'level': 'Рівень',
        'shift_type': 'ТипЗміни'
    }

    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM zp_worktime")
    worktime_rows = cursor.fetchall()

    cursor.execute("SELECT * FROM zp_фктРівніСпівробітників")
    levels_rows = cursor.fetchall()

    cursor.execute("SELECT * FROM zp_фктУмовиОплати")
    rules_rows = cursor.fetchall()

    levels_map = {}
    for lvl in levels_rows:
        key = (lvl['Прізвище'].strip().lower(), lvl['Посада'].strip().lower(), lvl['Відділення'].strip().lower())
        levels_map.setdefault(key, []).append(lvl)

    for work_row in worktime_rows:
        work_date = work_row['date_shift']
        last_name = work_row['last_name'].strip()
        position = work_row['position'].strip()
        department = work_row['department'].strip()

        key_specific = (last_name.lower(), position.lower(), department.lower())
        key_generic = (last_name.lower(), position.lower(), '')

        matched_levels = levels_map.get(key_specific, []) + levels_map.get(key_generic, [])
        matched_levels = [lvl for lvl in matched_levels
                          if lvl['ДатаПочатку'] <= work_date and
                          (lvl['ДатаЗакінчення'] is None or lvl['ДатаЗакінчення'] >= work_date)]

        error_messages, colision_messages = [], []
        level_value = None

        if len(matched_levels) > 1:
            error_messages.append("Колізія рівнів")
        elif len(matched_levels) == 1:
            level_value = matched_levels[0]['Рівень']
        else:
            error_messages.append("Рівень не знайдено")

        cursor.execute("""
            UPDATE zp_worktime SET level=%s WHERE date_shift=%s AND idx=%s
        """, (level_value, work_row['date_shift'], work_row['idx']))

        best_matches = []

        for rule in rules_rows:
            if not (rule['ДатаПочатку'] <= work_date and
                    (rule['ДатаЗакінчення'] is None or rule['ДатаЗакінчення'] >= work_date)):
                continue

            matches, score, skip = 0, 0, False
            for field, weight in weights.items():
                work_val = str(work_row.get(field, '')).strip().lower()
                rule_val = str(rule.get(field_mapping[field], '')).strip().lower()
                if rule_val:
                    if work_val == rule_val:
                        matches += 1
                        score += weight
                    else:
                        skip = True
                        break
            if not skip:
                best_matches.append({'rule': rule, 'matches': matches, 'score': score})

        if best_matches:
            best_matches.sort(key=lambda x: (-x['matches'], -x['score']))
            top = best_matches[0]
            same_top = [bm for bm in best_matches if bm['matches'] == top['matches'] and bm['score'] == top['score']]
            rule_ids = set(bm['rule']['Rule_ID'] for bm in same_top)
            if len(rule_ids) > 1:
                colision_messages.append(f"Колізія Rule_ID: {', '.join(map(str, rule_ids))}")

            if not colision_messages and not error_messages:
                cursor.execute("""
                    UPDATE zp_worktime SET Matches=%s, Score=%s, Colision=%s, СтавкаЗміна=%s, СтавкаГодина=%s, Rule_ID=%s, ErrorLog=%s
                    WHERE date_shift=%s AND idx=%s
                """, (
                    top['matches'], top['score'], '',
                    top['rule']['СтавкаЗміна'],
                    float(top['rule']['СтавкаЗміна']) / 12 if top['rule']['СтавкаЗміна'] else 0,
                    top['rule']['Rule_ID'],
                    '',
                    work_row['date_shift'], work_row['idx']
                ))
            else:
                cursor.execute("""
                    UPDATE zp_worktime SET Matches=%s, Score=%s, Colision=%s, СтавкаЗміна=%s, СтавкаГодина=%s, Rule_ID=%s, ErrorLog=%s
                    WHERE date_shift=%s AND idx=%s
                """, (
                    top['matches'], top['score'],
                    '\n'.join(colision_messages),
                    0.0, 0.0, None,
                    '\n'.join(error_messages),
                    work_row['date_shift'], work_row['idx']
                ))

    conn.commit()
    cursor.close()
    conn.close()
    log_message("[OK] Обробка завершена!")

# === MAIN ===
if __name__ == "__main__":
    sync_employee_levels()
    calculate_worktime_matches()
