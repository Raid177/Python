"""
Скрипт імпортує дані з Google Sheets у таблицю zp_фктУмовиОплати:
[OK] Додає колонку ID та присвоює порядкові номери
[OK] Завантажує дані
[OK] Перевіряє дублікати
[OK] Автоматично проставляє ДатаЗакінчення для попередніх умов (рядків) правила, якщо додається новий рядок із пізнішою датою
[OK] Формує стабільний Rule_ID для груп правил (по Посада, Відділення, Рівень, ТипЗміни, Прізвище)
[OK] Захищає стовпці ID, Rule_ID та перший рядок (шапку)
[OK] Завантажує дані у Google Sheets та MySQL (truncate + insert)
"""

import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# === Налаштування ===
TOKEN_PATH = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Google/token.json"
SPREADSHEET_ID = "19mfpQ8XgUSMpGNKQpLtL9ek5OrLj2UlvgUVR39yWubw"
SHEET_NAME = "фкт_УмовиОплати"
RANGE_NAME = f"{SHEET_NAME}!A1:Z"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

print(f"[START] Скрипт запущено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# === Крок 1. Отримати дані з Google Sheets ===
result = sheet.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME
).execute()
values = result.get('values', [])

if not values or len(values) < 2:
    print("[ERROR] Google Sheet порожній або відсутні дані.")
    exit(1)

header_len = len(values[0])
data_rows = []
for row in values[1:]:
    while len(row) < header_len:
        row.append('')
    data_rows.append(row)

df = pd.DataFrame(data_rows, columns=values[0])
print(f"[OK] Отримано {len(df)} рядків із Google Sheets.")

# === Крок 2. Додаємо/оновлюємо колонку ID ===
df['ID'] = range(1, len(df) + 1)

# === Крок 3. Завантажуємо ID назад у Google Sheets ===
values_to_upload = [list(df.columns)] + df.values.tolist()
sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=RANGE_NAME,
    valueInputOption='RAW',
    body={'values': values_to_upload}
).execute()
print("[OK] Колонка ID оновлена у Google Sheets.")

# === Крок 4. Додаємо захист для ID, Rule_ID та шапки ===
sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
sheet_id = sheet_metadata['sheets'][0]['properties']['sheetId']  # Перший лист

requests = [
    {
        "addProtectedRange": {
            "protectedRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1
                },
                "description": "Захищено: ID",
                "warningOnly": False
            }
        }
    },
    {
        "addProtectedRange": {
            "protectedRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startColumnIndex": 1,
                    "endColumnIndex": 2
                },
                "description": "Захищено: Rule_ID",
                "warningOnly": False
            }
        }
    },
    {
        "addProtectedRange": {
            "protectedRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1
                },
                "description": "Захищено: Шапка",
                "warningOnly": False
            }
        }
    }
]

service.spreadsheets().batchUpdate(
    spreadsheetId=SPREADSHEET_ID,
    body={"requests": requests}
).execute()
print("[OK] Діапазони захищено у Google Sheets.")

# === Крок 5. Обробка дат ===
df['ДатаПочатку'] = pd.to_datetime(df['ДатаПочатку'], dayfirst=True, errors='coerce')
df['ДатаЗакінчення'] = pd.to_datetime(df['ДатаЗакінчення'], dayfirst=True, errors='coerce')

# === Крок 6. Сортування по ID (щоб зберегти порядок користувача) ===
df.sort_values(by=['ID'], inplace=True)

# === Крок 7. Закриття рядків (з урахуванням АнЗП для умови правила) ===
changed_rows = 0
df_for_closing = df.sort_values(by=[
    'Посада', 'Відділення', 'Рівень', 'ТипЗміни', 'Прізвище', 'АнЗП', 'ДатаПочатку'
])

for idx in range(len(df_for_closing)):
    curr = df_for_closing.iloc[idx]
    for next_idx in range(idx + 1, len(df_for_closing)):
        next_row = df_for_closing.iloc[next_idx]
        if (
            curr['Посада'] == next_row['Посада'] and
            curr['Відділення'] == next_row['Відділення'] and
            curr['Рівень'] == next_row['Рівень'] and
            curr['ТипЗміни'] == next_row['ТипЗміни'] and
            curr['Прізвище'] == next_row['Прізвище'] and
            curr['АнЗП'] == next_row['АнЗП']
        ):
            if (
                pd.notna(next_row['ДатаПочатку']) and
                next_row['ДатаПочатку'] > curr['ДатаПочатку']
            ):
                new_end_date = next_row['ДатаПочатку'] - timedelta(days=1)
                if (pd.isna(curr['ДатаЗакінчення']) or
                    pd.to_datetime(curr['ДатаЗакінчення'], dayfirst=True) < new_end_date):
                    df.at[curr.name, 'ДатаЗакінчення'] = new_end_date.strftime('%d.%m.%Y')
                    changed_rows += 1
            break
print(f"[OK] Всього змінено ДатаЗакінчення у {changed_rows} рядках.")

# === Крок 8. Присвоюємо Rule_ID стабільно на основі ключа правила ===
rule_id_map = {}
current_rule_id = 1
df['Rule_ID'] = 0

for idx in range(len(df)):
    curr = df.iloc[idx]
    key = (
        str(curr['Посада']).strip().lower(),
        str(curr['Відділення']).strip().lower(),
        str(curr['Рівень']).strip().lower(),
        str(curr['ТипЗміни']).strip().lower(),
        str(curr['Прізвище']).strip().lower()
    )
    if key in rule_id_map:
        df.at[df.index[idx], 'Rule_ID'] = rule_id_map[key]
    else:
        rule_id_map[key] = current_rule_id
        df.at[df.index[idx], 'Rule_ID'] = current_rule_id
        current_rule_id += 1

print("[OK] Rule_ID стабільно присвоєно для всіх рядків.")

# === Крок 9. Форматуємо дати ===
df['ДатаПочатку'] = df['ДатаПочатку'].dt.strftime('%d.%m.%Y')
df['ДатаЗакінчення'] = df['ДатаЗакінчення'].apply(lambda x: x.strftime('%d.%m.%Y') if pd.notna(x) and x != '' else '')
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

# === Крок 11. Завантажуємо у БД ===
# Завантаження .env
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

DB_HOST = os.getenv("DB_HOST_Serv")
DB_PORT = int(os.getenv("DB_PORT_Serv", 3306))  # з дефолтом на всякий випадок
DB_USER = os.getenv("DB_USER_Serv")
DB_PASSWORD = os.getenv("DB_PASSWORD_Serv")
DB_DATABASE = os.getenv("DB_DATABASE_Serv")

connection = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)

df['ДатаПочатку'] = pd.to_datetime(df['ДатаПочатку'], dayfirst=True, errors='coerce')
df['ДатаЗакінчення'] = pd.to_datetime(df['ДатаЗакінчення'], dayfirst=True, errors='coerce')

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
    for _, row in df.iterrows():
        cursor.execute(insert_sql, (
            row['Rule_ID'],
            row['ДатаПочатку'] if pd.notna(row['ДатаПочатку']) else None,
            row['ДатаЗакінчення'] if pd.notna(row['ДатаЗакінчення']) else None,
            row['Посада'],
            row['Відділення'],
            row['Рівень'],
            row['ТипЗміни'],
            row['Прізвище'],
            float(str(row.get('СтавкаЗміна', 0)).replace(',', '.')) if row.get('СтавкаЗміна', '') else 0,
            row.get('АнЗП', ''),
            float(str(row.get('Ан_Призначив', 0)).replace(',', '.')) if row.get('Ан_Призначив', '') else 0,
            float(str(row.get('Ан_Виконав', 0)).replace(',', '.')) if row.get('Ан_Виконав', '') else 0,
            row.get('АнЗП_Колективний', ''),
            float(str(row.get('Ан_Колективний', 0)).replace(',', '.')) if row.get('Ан_Колективний', '') else 0
        ))
        inserted_rows += 1

    connection.commit()

print(f"[OK] Дані перезаписані у БД (zp_фктУмовиОплати). Вставлено {inserted_rows} рядків.")
