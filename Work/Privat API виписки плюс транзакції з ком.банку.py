import requests
from dotenv import load_dotenv
import os
import pymysql
import re
from datetime import datetime, timedelta

# Завантажуємо змінні з .env файлу
load_dotenv()

# Дані для підключення до БД
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "cursorclass": pymysql.cursors.DictCursor
}

# Номери рахунків для кожного ФОП
accounts_fop1 = ['UA973052990000026002025035545']
accounts_fop2 = ['UA173375460000026000045200003', 'UA453052990000026004005203890']

# Токени для кожного ФОП
tokens = {
    'FOP1': os.getenv('API_TOKEN_LOV'),
    'FOP2': os.getenv('API_TOKEN_ZVO'),
}

# Глобальний список нових полів
global_new_fields = {}
global_field_samples = {}

def guess_mysql_type(values):
    non_nulls = [v for v in values if v is not None]
    if not non_nulls:
        return "TEXT"
    if all(isinstance(v, int) or (isinstance(v, str) and v.isdigit()) for v in non_nulls):
        return "BIGINT"
    if all(re.match(r'^-?\\d+\\.\\d+$', str(v)) for v in non_nulls):
        return "DECIMAL(18,4)"
    if all(re.match(r'^\\d{4}-\\d{2}-\\d{2}$', str(v)) for v in non_nulls):
        return "DATE"
    if all(re.match(r'^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$', str(v)) for v in non_nulls):
        return "DATETIME"
    if max(len(str(v)) for v in non_nulls) <= 255:
        return "VARCHAR(255)"
    return "TEXT"

def ensure_columns_exist(table_name, sample_record, cursor):
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    existing_columns = set(row["Field"] for row in cursor.fetchall())
    added_fields = []

    for field in sample_record:
        if field not in existing_columns:
            global_field_samples.setdefault(field, []).append(sample_record[field])
            if len(global_field_samples[field]) >= 10:
                guessed_type = guess_mysql_type(global_field_samples[field])
                cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{field}` {guessed_type} NULL")
                added_fields.append(field)
                global_new_fields.setdefault(field, set()).add(table_name)
    return added_fields

def extract_commission(osnd_text):
    match = re.search(r"Ком бан ([\d.]+)грн", osnd_text)
    return float(match.group(1)) if match else 0.0

def fetch_and_save_transactions(account_number, token, start_date, end_date, connection):
    url = 'https://acp.privatbank.ua/api/statements/transactions'
    headers = {
        'User-Agent': 'PythonClient',
        'token': token,
        'Content-Type': 'application/json;charset=cp1251'
    }
    params = {
        'acc': account_number,
        'startDate': start_date,
        'endDate': end_date,
        'limit': '50'
    }

    next_page_id = None
    with connection.cursor() as cursor:
        while True:
            if next_page_id:
                params['followId'] = next_page_id

            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'SUCCESS':
                    transactions = data.get('transactions', [])
                    if transactions:
                        for transaction in transactions:
                            try:
                                transaction['DATE_TIME_DAT_OD_TIM_P'] = datetime.strptime(transaction['DATE_TIME_DAT_OD_TIM_P'], '%d.%m.%Y %H:%M:%S') if transaction.get('DATE_TIME_DAT_OD_TIM_P') else None
                                transaction['DAT_OD'] = datetime.strptime(transaction['DAT_OD'], '%d.%m.%Y').date() if transaction.get('DAT_OD') else None
                            except Exception as e:
                                print(f"❌ Помилка при обробці дати/часу: {e}")
                                continue

                            ensure_columns_exist("bnk_trazact_prvt", transaction, cursor)
                            ensure_columns_exist("bnk_trazact_prvt_ekv", transaction, cursor)

                            placeholders = ", ".join(["%s"] * len(transaction))
                            columns = ", ".join(f"`{k}`" for k in transaction.keys())
                            sql = f"""INSERT INTO bnk_trazact_prvt ({columns}) \
                                      VALUES ({placeholders}) \
                                      ON DUPLICATE KEY UPDATE \
                                      DATE_TIME_DAT_OD_TIM_P = VALUES(DATE_TIME_DAT_OD_TIM_P),\
                                      DAT_OD = VALUES(DAT_OD)"""
                            cursor.execute(sql, tuple(transaction.values()))

                        connection.commit()
                        print(f"✅ {len(transactions)} транзакцій для рахунку {account_number} збережено у БД")
                    else:
                        print(f"❌ Немає нових транзакцій для рахунку {account_number}")

                    if data.get('exist_next_page'):
                        next_page_id = data.get('next_page_id')
                        print(f"🔄 Наступна сторінка для рахунку {account_number}: {next_page_id}")
                    else:
                        print(f"✅ Завершено отримання транзакцій для рахунку {account_number}")
                        break
                else:
                    print(f"❌ Помилка отримання даних: {data.get('message')}")
                    break
            else:
                print(f"❌ HTTP {response.status_code}: {response.text}")
                break

def migrate_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT MAX(DAT_OD) FROM bnk_trazact_prvt_ekv")
        last_date = cursor.fetchone()['MAX(DAT_OD)']

        if last_date is None:
            last_date = datetime(2024, 7, 1).date()
        else:
            last_date -= timedelta(days=1)

        cursor.execute("SELECT * FROM bnk_trazact_prvt WHERE DAT_OD >= %s", (last_date,))
        rows = cursor.fetchall()

        for row in rows:
            new_rows = [row.copy()]

            if row['AUT_CNTR_NAM'] == "Розрахунки з еквайрингу" and row['OSND'].startswith("cmps: 12"):
                modified_row = row.copy()
                modified_row['NUM_DOC'] += "_ek"
                modified_row['TRANTYPE'] = "D" if row['TRANTYPE'] == "C" else row['TRANTYPE']
                modified_row['OSND'] = "Розрахунки з еквайрингом"
                modified_row['SUM'] = extract_commission(row['OSND'])
                modified_row['SUM_E'] = modified_row['SUM']
                new_rows.append(modified_row)

            for new_row in new_rows:
                placeholders = ", ".join(["%s"] * len(new_row))
                columns = ", ".join(f"`{k}`" for k in new_row.keys())
                sql = f"""INSERT INTO bnk_trazact_prvt_ekv ({columns}) \
                          VALUES ({placeholders}) \
                          ON DUPLICATE KEY UPDATE \
                          DATE_TIME_DAT_OD_TIM_P = VALUES(DATE_TIME_DAT_OD_TIM_P),\
                          DAT_OD = VALUES(DAT_OD),\
                          SUM = VALUES(SUM),\
                          SUM_E = VALUES(SUM_E)"""
                cursor.execute(sql, tuple(new_row.values()))

        connection.commit()
        print(f"✅ Перенесено {len(rows)} записів.")

if __name__ == "__main__":
    connection = pymysql.connect(**DB_CONFIG)

    try:
        for fop, token in tokens.items():
            print(f"🔑 Використовується токен для {fop}")
            for account in (accounts_fop1 if fop == 'FOP1' else accounts_fop2):
                print(f"📅 Одержання транзакцій для рахунку {account}")

                with connection.cursor() as cursor:
                    cursor.execute("SELECT MAX(DATE_TIME_DAT_OD_TIM_P) FROM bnk_trazact_prvt WHERE AUT_MY_ACC = %s", (account,))
                    last_date = cursor.fetchone()['MAX(DATE_TIME_DAT_OD_TIM_P)']

                if last_date:
                    start_date = (last_date - timedelta(days=1)).strftime('%d-%m-%Y')
                else:
                    start_date = '01-07-2024'

                end_date = datetime.now().strftime('%d-%m-%Y')
                fetch_and_save_transactions(account, token, start_date, end_date, connection)

        migrate_data(connection)

        if global_new_fields:
            print("\n🚨 !НОВІ ПОЛЯ ДОДАНО!")
            for field, tables in sorted(global_new_fields.items()):
                print(f"   ➕ {field} (таблиці: {', '.join(tables)})")

    finally:
        connection.close()