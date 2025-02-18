import requests
import pymysql
import os
import re
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Завантажуємо змінні середовища
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
accounts_fop2 = ['UA173375460000026000045200003']

# Токени для кожного ФОП
tokens = {
    'FOP1': os.getenv('API_TOKEN_LOV'),
    'FOP2': os.getenv('API_TOKEN_ZVO'),
}

def extract_commission(osnd_text):
    """Витягуємо суму комісії з тексту OSND."""
    match = re.search(r"Ком бан ([\d.]+)грн", osnd_text)
    return float(match.group(1)) if match else 0.0

def fetch_and_save_transactions():
    """Отримання та збереження транзакцій із ПриватБанку."""
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            for fop, token in tokens.items():
                for account in (accounts_fop1 if fop == 'FOP1' else accounts_fop2):
                    cursor.execute("SELECT MAX(DATE_TIME_DAT_OD_TIM_P) FROM bnk_trazact_prvt WHERE AUT_MY_ACC = %s", (account,))
                    last_date = cursor.fetchone()["MAX(DATE_TIME_DAT_OD_TIM_P)"]
                    start_date = (last_date - timedelta(days=1)).strftime('%d-%m-%Y') if last_date else '01-07-2024'
                    end_date = datetime.now().strftime('%d-%m-%Y')
                    
                    url = 'https://acp.privatbank.ua/api/statements/transactions'
                    headers = {'User-Agent': 'PythonClient', 'token': token, 'Content-Type': 'application/json;charset=cp1251'}
                    params = {'acc': account, 'startDate': start_date, 'endDate': end_date, 'limit': '50'}
                    response = requests.get(url, headers=headers, params=params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('status') == 'SUCCESS':
                            transactions = data.get('transactions', [])
                            for transaction in transactions:
                                cursor.execute("""
                                    INSERT INTO bnk_trazact_prvt (%s) 
                                    VALUES (%s)
                                    ON DUPLICATE KEY UPDATE DATE_TIME_DAT_OD_TIM_P = VALUES(DATE_TIME_DAT_OD_TIM_P), DAT_OD = VALUES(DAT_OD)
                                """ % (', '.join(transaction.keys()), ', '.join(['%s'] * len(transaction))), tuple(transaction.values()))
                    connection.commit()
    finally:
        connection.close()

def migrate_data():
    """Переносимо дані між таблицями."""
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(DAT_OD) FROM bnk_trazact_prvt_ekv")
            max_date = cursor.fetchone()["MAX(DAT_OD)"]
            start_date = max_date - timedelta(days=1) if max_date else datetime(2024, 7, 1).date()
            cursor.execute("SELECT * FROM bnk_trazact_prvt WHERE DAT_OD >= %s", (start_date,))
            rows = cursor.fetchall()
            
            for row in rows:
                new_rows = [row.copy()]
                if row['AUT_CNTR_NAM'] == "Розрахунки з еквайрингу" and row['OSND'].startswith("cmps: 12"):
                    modified_row = row.copy()
                    modified_row['NUM_DOC'] += "_ek"
                    modified_row['TRANTYPE'] = "D" if row['TRANTYPE'] == "C" else row['TRANTYPE']
                    modified_row['OSND'] = "Комісія банку за еквайринг"
                    modified_row['SUM'] = extract_commission(row['OSND'])
                    modified_row['SUM_E'] = modified_row['SUM']
                    new_rows.append(modified_row)
                
                for new_row in new_rows:
                    placeholders = ", ".join(["%s"] * len(new_row))
                    columns = ", ".join(new_row.keys())
                    sql = f"INSERT INTO bnk_trazact_prvt_ekv ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE SUM=VALUES(SUM), SUM_E=VALUES(SUM_E)"
                    cursor.execute(sql, tuple(new_row.values()))
            connection.commit()
    finally:
        connection.close()

if __name__ == "__main__":
    print("📥 Завантажуємо транзакції з ПриватБанку...")
    fetch_and_save_transactions()
    print("✅ Завантаження завершено!")
    print("🔄 Виконуємо міграцію транзакцій...")
    migrate_data()
    print("✅ Міграція завершена!")
