# -*- coding: utf-8 -*-

"""
Скрипт імпортує дані з Google Sheets у таблицю zp_фктУмовиОплати:
[OK] Додає колонку ID та присвоює порядкові номери
[OK] Завантажує дані
[OK] Перевіряє дублікати
[OK] Автоматично проставляє ДатаЗакінчення для попередніх умов (рядків) правила, якщо додається новий рядок із пізнішою датою
[OK] Формує стабільний Rule_ID для груп правил (по Посада, Відділення, Рівень, ТипЗміни, Прізвище)
[OK] Захищає стовпці ID, Rule_ID та перший рядок (шапку)
[OK] Завантажує дані у Google Sheets та MySQL (truncate + insert)
Авторизація: service account JSON
"""

import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
from datetime import datetime, timedelta

# === Google Sheets API (SERVICE ACCOUNT) ===
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# === Налаштування ===
SERVICE_ACCOUNT_FILE = "/root/Python/_Acces/zppetwealth-770254b6d8c1.json"  # ваш файл service account
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_УмовиОплати"
RANGE_NAME = f"{SHEET_NAME}!A1:Z"

print(f"[START] Скрипт запущено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ========= ENV loader (ваш блок) =========
ENV_PROFILE = os.getenv("ENV_PROFILE", "prod")  # dev | prod
ENV_PATHS = {
    "dev":  r"C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env",
    "prod": "/root/Python/_Acces/.env.prod",
}
ENV_PATH = os.getenv("ENV_PATH") or ENV_PATHS.get(ENV_PROFILE)
if ENV_PATH and os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)
else:
    load_dotenv(override=True)  # пробує .env в поточній папці

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"[ENV ERROR] Missing {name}. Check {ENV_PATH or '.env'}.")
    return v

# === Авторизація Google (service account) ===
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# === Крок 1. Отримати дані з Google Sheets ===
result = sheet.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME
).execute()
values = result.get('values', [])

if not values or len(values) < 2:
    print("[ERROR] Google Sheet порожній або відсутні дані.")
    raise SystemExit(1)

header = values[0]
header_len = len(header)
data_rows = []
for row in values[1:]:
    row = list(row)
    while len(row) < header_len:
        row.append('')
    data_rows.append(row)

df = pd.DataFrame(data_rows, columns=header)
print(f"[OK] Отримано {len(df)} рядків із Google Sheets.")

# === Крок 2. Додаємо/оновлюємо колонку ID ===
df['ID'] = range(1, len(df) + 1)

# === Крок 3. Завантажуємо ID назад у Google Sheets ===
values_to_upload = [list(df.columns)] + df.astype(str).values.tolist()
sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME,
    valueInputOption='RAW',
    body={'values': values_to_upload}
).execute()
print("[OK] Колонка ID оновлена у Google Sheets.")

# === Крок 4. Додаємо захист для ID, Rule_ID та шапки ===
# коректно знаходимо sheetId саме за назвою листа
smeta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
sheet_id = None
for s in smeta.get("sheets", []):
    if s["properties"].get("title") == SHEET_NAME:
        sheet_id = s["properties"]["sheetId"]
        break
if sheet_id is None:
    raise SystemExit(f"[ERROR] Не знайдено лист '{SHEET_NAME}' у таблиці.")

def find_column_index(cols, name, fallback):
    return cols.index(name) if name in cols else fallback

id_col_idx = find_column_index(list(df.columns), "ID", 0)
rule_col_idx = find_column_index(list(df.columns), "Rule_ID", 1)

requests = [
    {
        "addProtectedRange": {
            "protectedRange": {
                "range": {"sheetId": sheet_id, "startColumnIndex": id_col_idx, "endColumnIndex": id_col_idx + 1},
                "description": "Захищено: ID",
                "warningOnly": False
            }
        }
    },
    {
        "addProtectedRange": {
            "protectedRange": {
                "range": {"sheetId": sheet_id, "startColumnIndex": rule_col_idx, "endColumnIndex": rule_col_idx + 1},
                "description": "Захищено: Rule_ID",
                "warningOnly": False
            }
        }
    },
    {
        "addProtectedRange": {
            "protectedRange": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "description": "Захищено: Шапка",
                "warningOnly": False
            }
        }
    }
]

try:
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests}
    ).execute()
    print("[OK] Діапазони захищено у Google Sheets.")
except HttpError as e:
    # Якщо вже захищено — нехай буде попередження, але не фатально
    print(f"[WARN] Не вдалося додати захист (можливо, вже існує): {e}")

# === Крок 5. Обробка дат ===
df['ДатаПочатку'] = pd.to_datetime(df['ДатаПочатку'], dayfirst=True, errors='coerce')
df['ДатаЗакінчення'] = pd.to_datetime(df['ДатаЗакінчення'], dayfirst=True, errors='coerce')

# === Крок 6. Сортування по ID (щоб зберегти порядок користувача) ===
df.sort_values(by=['ID'], inplace=True)

# === Крок 7. Закриття рядків (враховує АнЗП у ключі правила) ===
changed_rows = 0
df_for_closing = df.sort_values(by=[
    'Посада', 'Відділення', 'Рівень', 'ТипЗміни', 'Прізвище', 'АнЗП', 'ДатаПочатку', 'ID'
])

for idx in range(len(df_for_closing) - 1):
    curr = df_for_closing.iloc[idx]
    next_row = df_for_closing.iloc[idx + 1]
    same_group = (
        curr['Посада'] == next_row['Посада'] and
        curr['Відділення'] == next_row['Відділення'] and
        curr['Рівень'] == next_row['Рівень'] and
        curr['ТипЗміни'] == next_row['ТипЗміни'] and
        curr['Прізвище'] == next_row['Прізвище'] and
        curr['АнЗП'] == next_row['АнЗП']
    )
    if same_group:
        if pd.notna(next_row['ДатаПочатку']) and pd.notna(curr['ДатаПочатку']) and next_row['ДатаПочатку'] > curr['ДатаПочатку']:
            new_end_date = next_row['ДатаПочатку'] - timedelta(days=1)
            if (pd.isna(curr['ДатаЗакінчення']) or curr['ДатаЗакінчення'] < new_end_date):
                # оновлюємо в оригінальному df за індексом curr.name
                df.loc[curr.name, 'ДатаЗакінчення'] = new_end_date
                changed_rows += 1

print(f"[OK] Всього змінено ДатаЗакінчення у {changed_rows} рядках.")

# === Крок 8. Присвоюємо Rule_ID стабільно на основі ключа правила ===
rule_id_map = {}
current_rule_id = 1
df['Rule_ID'] = 0

for ridx in range(len(df)):
    curr = df.iloc[ridx]
    key = (
        str(curr['Посада']).strip().lower(),
        str(curr['Відділення']).strip().lower(),
        str(curr['Рівень']).strip().lower(),
        str(curr['ТипЗміни']).strip().lower(),
        str(curr['Прізвище']).strip().lower()
    )
    if key in rule_id_map:
        df.at[df.index[ridx], 'Rule_ID'] = rule_id_map[key]
    else:
        rule_id_map[key] = current_rule_id
        df.at[df.index[ridx], 'Rule_ID'] = current_rule_id
        current_rule_id += 1

print("[OK] Rule_ID стабільно присвоєно для всіх рядків.")

# === Крок 9. Форматуємо дати для вивантаження назад у Sheet ===
df['ДатаПочатку'] = df['ДатаПочатку'].apply(lambda x: x.strftime('%d.%m.%Y') if pd.notna(x) else '')
df['ДатаЗакінчення'] = df['ДатаЗакінчення'].apply(lambda x: x.strftime('%d.%m.%Y') if pd.notna(x) else '')
df = df.fillna('')

# === Крок 10. Оновлюємо дані у Google Sheets ===
values_to_upload = [list(df.columns)] + df.values.tolist()
sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME,
    valueInputOption='RAW',
    body={'values': values_to_upload}
).execute()
print("[OK] Дані оновлені у Google Sheets.")

# === Крок 10.1. Перевірка дублів за унікальним ключем (зупиняє скрипт) ===
# Ключ групи правил = ці колонки:
UNIQUE_KEY_COLS = [
    "ДатаПочатку", "Посада", "Відділення", "Рівень",
    "ТипЗміни", "Прізвище", "АнЗП", "АнЗП_Колективний"
]

YES_SET = {"так", "yes", "true", "1"}
NO_SET  = {"ні", "нет", "no", "false", "0"}

def _norm_val(v):
    """Нормалізація: ''/None -> None; дати -> 'YYYY-MM-DD'; текст -> нижній регістр+strip; 'так/ні' уніфікуємо."""
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("none", "null", "nan"):
        return None
    # дати
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.date().isoformat()
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.notna(ts):
            return ts.date().isoformat()
    except Exception:
        pass
    # уніфікація булевих-подібних
    low = s.lower()
    if low in YES_SET: return "так"
    if low in NO_SET:  return "ні"
    return low  # текст порівнюємо у нижньому регістрі

missing = [c for c in UNIQUE_KEY_COLS if c not in df.columns]
if missing:
    raise SystemExit(f"[ERROR] В аркуші немає колонок ключа: {', '.join(missing)}")

df["_uniq_key"] = df[UNIQUE_KEY_COLS].apply(
    lambda r: tuple(_norm_val(r[c]) for c in UNIQUE_KEY_COLS), axis=1
)

dup_groups = df.groupby("_uniq_key").size()
dup_groups = dup_groups[dup_groups > 1]

if len(dup_groups) > 0:
    print("\n[ERROR] Виявлено дублікати за ключем групи правил (скрипт зупинено).")
    shown = 0
    for key_tuple, cnt in dup_groups.items():
        idxs = df.index[df["_uniq_key"] == key_tuple].tolist()
        ids = df.loc[idxs, "ID"].astype(str).tolist() if "ID" in df.columns else [str(i+2) for i in idxs]
        key_pretty = ", ".join(f"{col}={repr(val)}" for col, val in zip(UNIQUE_KEY_COLS, key_tuple))
        print(f"[DUP] ({key_pretty}) | К-сть: {cnt} | Рядки ID: {', '.join(ids)}")
        shown += 1
        if shown >= 50:
            print("[INFO] ...показано перші 50 груп.")
            break
    raise SystemExit(2)

# === Крок 11. Завантажуємо у БД ===
DB_HOST = require_env("DB_HOST")
DB_PORT = int(require_env("DB_PORT"))  # порт обов'язково є у .env
DB_USER = require_env("DB_USER")
DB_PASSWORD = require_env("DB_PASSWORD")
DB_DATABASE = require_env("DB_DATABASE")

connection = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE,
    charset="utf8mb4",
    autocommit=False
)

# повертаємо дати в datetime для інсерту
df_db = df.copy()
df_db['ДатаПочатку'] = pd.to_datetime(df_db['ДатаПочатку'], dayfirst=True, errors='coerce')
df_db['ДатаЗакінчення'] = pd.to_datetime(df_db['ДатаЗакінчення'], dayfirst=True, errors='coerce')

with connection.cursor() as cursor:
    cursor.execute("TRUNCATE TABLE zp_фктУмовиОплати")
    insert_sql = """
        INSERT INTO zp_фктУмовиОплати (
            Rule_ID, ДатаПочатку, ДатаЗакінчення, Посада, Відділення, Рівень,
            ТипЗміни, Прізвище, СтавкаЗміна, АнЗП,
            Ан_Призначив, Ан_Виконав, АнЗП_Колективний, Ан_Колективний
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    inserted_rows = 0
    for _, row in df_db.iterrows():
        # числові: заміна коми на крапку + пусте -> 0
        def num(v):
            s = str(v).strip()
            if not s:
                return 0.0
            s = s.replace(' ', '').replace(',', '.')
            try:
                return float(s)
            except ValueError:
                return 0.0

        cursor.execute(insert_sql, (
            int(row['Rule_ID']) if str(row['Rule_ID']).isdigit() else 0,
            row['ДатаПочатку'] if pd.notna(row['ДатаПочатку']) else None,
            row['ДатаЗакінчення'] if pd.notna(row['ДатаЗакінчення']) else None,
            row.get('Посада', ''),
            row.get('Відділення', ''),
            row.get('Рівень', ''),
            row.get('ТипЗміни', ''),
            row.get('Прізвище', ''),
            num(row.get('СтавкаЗміна', '')),
            row.get('АнЗП', ''),
            num(row.get('Ан_Призначив', '')),
            num(row.get('Ан_Виконав', '')),
            row.get('АнЗП_Колективний', ''),
            num(row.get('Ан_Колективний', '')),
        ))
        inserted_rows += 1

    connection.commit()

print(f"[OK] Дані перезаписані у БД (zp_фктУмовиОплати). Вставлено {inserted_rows} рядків.")
