import requests
from dotenv import load_dotenv
import os
import mysql.connector
from datetime import datetime, timedelta

# Завантажуємо змінні з .env файлу
load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

# Підключення до БД
conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
cursor = conn.cursor()

# Отримуємо дату останнього запису
cursor.execute("SELECT MAX(DATE_TIME_DAT_OD_TIM_P) FROM bnk_trazact_prvt")
last_date = cursor.fetchone()[0]
if not last_date:
    last_date = datetime.now() - timedelta(days=30)
start_date = (last_date - timedelta(days=1)).strftime('%d-%m-%Y')
end_date = datetime.now().strftime('%d-%m-%Y')

# API-запит
url = 'https://acp.privatbank.ua/api/statements/transactions'
headers = {
    'User-Agent': 'PythonClient',
    'token': API_TOKEN,
    'Content-Type': 'application/json;charset=cp1251'
}
params = {
    'acc': 'UA973052990000026002025035545',
    'startDate': start_date,
    'endDate': end_date,
    'limit': '100'
}
response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    if data.get('status') == 'SUCCESS':
        transactions = data.get('transactions', [])
        for transaction in transactions:
            cursor.execute("""
                INSERT INTO bnk_trazact_prvt (%s) 
                VALUES (%s)
                ON DUPLICATE KEY UPDATE
                DATE_TIME_DAT_OD_TIM_P = VALUES(DATE_TIME_DAT_OD_TIM_P)
            """ % (
                ', '.join(transaction.keys()),
                ', '.join(['%s'] * len(transaction))
            ), tuple(transaction.values()))
        conn.commit()
        print("✅ Транзакції успішно збережено у БД")
    else:
        print("❌ Помилка отримання транзакцій:", data.get('message'))
else:
    print(f"❌ Помилка {response.status_code}: {response.text}")

cursor.close()
conn.close()
