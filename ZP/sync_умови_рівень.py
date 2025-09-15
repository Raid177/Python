# -*- coding: utf-8 -*-
"""
Скрипт для підтягування рівнів співробітників та розрахунку умов оплати у таблиці zp_worktime.

Блок 1: синхронізація таблиці "фкт_РівніСпівробітників" з Google Sheets у MySQL.
Блок 2: оновлення полів у zp_worktime на основі рівнів і правил (zp_фктУмовиОплати).

Зміни в цій версії:
- Авторизація Google: service account JSON (/root/Python/_Acces/zppetwealth-770254b6d8c1.json)
- ENV loader з require_env() і ключами DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_DATABASE
"""

import os
import sys
import pymysql
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# === Google Sheets API (SERVICE ACCOUNT) ===
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- ENV loader ----------
ENV_PROFILE = os.getenv("ENV_PROFILE", "prod")  # dev | prod
ENV_PATHS = {
    "dev":  r"C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env",
    "prod": "/root/Python/_Acces/.env.prod",
}
ENV_PATH = os.getenv("ENV_PATH") or ENV_PATHS.get(ENV_PROFILE)
if ENV_PATH and os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)
else:
    load_dotenv(override=True)  # пробує .env у поточній папці

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"[ENV ERROR] Missing {name}. Check {ENV_PATH or '.env'}.")
    return v

# ---------- DB config (увага на назви змінних) ----------
DB_CONFIG = {
    "host": require_env("DB_HOST"),
    "port": int(require_env("DB_PORT")),
    "user": require_env("DB_USER"),
    "password": require_env("DB_PASSWORD"),
    "database": require_env("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

# ---------- Google Sheets ----------
SERVICE_ACCOUNT_FILE = "/root/Python/_Acces/zppetwealth-770254b6d8c1.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPREADSHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_РівніСпівробітників"
RANGE_NAME = f"{SHEET_NAME}!A1:Z"

def build_sheets_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds).spreadsheets()

def log_message(message: str):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}")

# ---------- Block 1: sync_employee_levels ----------
def sync_employee_levels():
    log_message("[SYNC] Початок синхронізації рівнів співробітників...")
    sheet = build_sheets_service()

    try:
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    except HttpError as e:
        log_message(f"[ERROR] Google API error: {e}")
        sys.exit(1)

    values = result.get("values", [])
    if not values:
        log_message("[ERROR] Дані не знайдено в Google Sheets!")
        return

    header = values[0]
    data = values[1:]
    df = pd.DataFrame(data, columns=header)

    # Дати
    df["ДатаПочатку"] = pd.to_datetime(df["ДатаПочатку"], dayfirst=True, errors="coerce")
    df["ДатаЗакінчення"] = pd.to_datetime(df["ДатаЗакінчення"], dayfirst=True, errors="coerce")

    # Тримм
    for col in ["Прізвище", "Посада", "Відділення", "Рівень"]:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else "")

    # Дублікати
    duplicate_mask = df.duplicated(subset=["Прізвище", "ДатаПочатку", "Посада", "Відділення", "Рівень"], keep=False)
    if duplicate_mask.any():
        log_message("[WARN] В Google Sheets знайдено дублікати, виправте їх перед завантаженням.")
        for _, row in df[duplicate_mask].iterrows():
            log_message(f"  - {row['Прізвище']} | {row['ДатаПочатку']} | {row['Посада']} | {row['Відділення']} | {row['Рівень']}")
        sys.exit(1)

    # Сортування
    df.sort_values(by=["Прізвище", "Посада", "Відділення", "ДатаПочатку"], inplace=True)

    # Закриття попередніх рівнів (перехід на новий)
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

    # Підготовка до запису назад у Sheets (формат дат dd.mm.yyyy)
    updated_values = [header]
    for _, row in df.iterrows():
        row_out = []
        for col in header:
            val = row.get(col, "")
            if isinstance(val, pd.Timestamp):
                row_out.append("" if pd.isnull(val) else val.strftime("%d.%m.%Y"))
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

    # Завантаження у БД (ON DUPLICATE KEY UPDATE)
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
        # rowcount: 1 -> insert, 2 -> update
        if cursor.rowcount == 1:
            inserted += 1
        elif cursor.rowcount == 2:
            updated += 1

    conn.commit()
    cursor.close()
    conn.close()
    log_message(f"[OK] Завантажено: {inserted} додано, {updated} оновлено.")

# ---------- Block 2: calculate_worktime_matches ----------
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

    # Індекс рівнів
    levels_map = {}
    for lvl in levels_rows:
        key = ( (lvl.get('Прізвище') or '').strip().lower(),
                (lvl.get('Посада') or '').strip().lower(),
                (lvl.get('Відділення') or '').strip().lower() )
        levels_map.setdefault(key, []).append(lvl)

    for work_row in worktime_rows:
        work_date = work_row['date_shift']
        last_name = (work_row.get('last_name') or '').strip()
        position = (work_row.get('position') or '').strip()
        department = (work_row.get('department') or '').strip()

        key_specific = (last_name.lower(), position.lower(), department.lower())
        key_generic = (last_name.lower(), position.lower(), '')

        matched_levels = levels_map.get(key_specific, []) + levels_map.get(key_generic, [])
        matched_levels = [
            lvl for lvl in matched_levels
            if lvl['ДатаПочатку'] <= work_date and
               (lvl['ДатаЗакінчення'] is None or lvl['ДатаЗакінчення'] >= work_date)
        ]

        error_messages, colision_messages = [], []
        level_value = None

        if len(matched_levels) > 1:
            error_messages.append("Колізія рівнів")
        elif len(matched_levels) == 1:
            level_value = matched_levels[0]['Рівень']
        else:
            error_messages.append("Рівень не знайдено")

        # оновити level у zp_worktime
        cursor.execute("""
            UPDATE zp_worktime SET level=%s WHERE date_shift=%s AND idx=%s
        """, (level_value, work_row['date_shift'], work_row['idx']))

        # Підбір правил
        best_matches = []
        for rule in rules_rows:
            if not (rule['ДатаПочатку'] <= work_date and
                    (rule['ДатаЗакінчення'] is None or rule['ДатаЗакінчення'] >= work_date)):
                continue

            matches, score, skip = 0, 0, False
            for field, weight in weights.items():
                work_val = str(work_row.get(field, '') or '').strip().lower()
                rule_val = str(rule.get(field_mapping[field], '') or '').strip().lower()
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

            # ставка на зміну/годину
            def to_float(v):
                try:
                    return float(v) if v is not None and v != '' else 0.0
                except Exception:
                    try:
                        return float(str(v).replace(',', '.'))
                    except Exception:
                        return 0.0

            if not colision_messages and not error_messages:
                cursor.execute("""
                    UPDATE zp_worktime
                    SET Matches=%s, Score=%s, Colision=%s, СтавкаЗміна=%s, СтавкаГодина=%s, Rule_ID=%s, ErrorLog=%s
                    WHERE date_shift=%s AND idx=%s
                """, (
                    top['matches'], top['score'], '',
                    to_float(top['rule']['СтавкаЗміна']),
                    to_float(top['rule']['СтавкаЗміна']) / 12 if to_float(top['rule']['СтавкаЗміна']) else 0.0,
                    top['rule']['Rule_ID'],
                    '',
                    work_row['date_shift'], work_row['idx']
                ))
            else:
                cursor.execute("""
                    UPDATE zp_worktime
                    SET Matches=%s, Score=%s, Colision=%s, СтавкаЗміна=%s, СтавкаГодина=%s, Rule_ID=%s, ErrorLog=%s
                    WHERE date_shift=%s AND idx=%s
                """, (
                    top['matches'], top['score'],
                    '\n'.join(colision_messages) if colision_messages else '',
                    0.0, 0.0, None,
                    '\n'.join(error_messages) if error_messages else '',
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
