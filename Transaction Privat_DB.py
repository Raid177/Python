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
    'limit': '50'  # Одержуємо перші 50 записів
}

# Перевірка на наявність наступної пачки
next_page_id = None
while True:
    if next_page_id:
        params['followId'] = next_page_id  # Додаємо followId для отримання наступної пачки
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if data.get('status') == 'SUCCESS':
            transactions = data.get('transactions', [])
            if transactions:
                for transaction in transactions:
                    try:
                        # Перетворюємо дати
                        if 'DATE_TIME_DAT_OD_TIM_P' in transaction and transaction['DATE_TIME_DAT_OD_TIM_P']:
                            transaction['DATE_TIME_DAT_OD_TIM_P'] = datetime.strptime(transaction['DATE_TIME_DAT_OD_TIM_P'], '%d.%m.%Y %H:%M:%S')
                        else:
                            transaction['DATE_TIME_DAT_OD_TIM_P'] = None
                        
                        if 'DAT_OD' in transaction and transaction['DAT_OD']:
                            transaction['DAT_OD'] = datetime.strptime(transaction['DAT_OD'], '%d.%m.%Y').date()
                        else:
                            transaction['DAT_OD'] = None
                    except Exception as e:
                        print(f"❌ Помилка при обробці дати/часу: {e}")
                        continue
                    
                    # Зберігаємо транзакцію в БД
                    cursor.execute(""" 
                        INSERT INTO bnk_trazact_prvt (%s) 
                        VALUES (%s)
                        ON DUPLICATE KEY UPDATE 
                        DATE_TIME_DAT_OD_TIM_P = VALUES(DATE_TIME_DAT_OD_TIM_P),
                        DAT_OD = VALUES(DAT_OD)
                    """ % (
                        ', '.join(transaction.keys()),
                        ', '.join(['%s'] * len(transaction))
                    ), tuple(transaction.values()))
                
                conn.commit()
                print(f"✅ Пачка з {len(transactions)} транзакцій успішно збережена у БД")
            else:
                print("❌ Немає транзакцій для цього запиту.")
            
            # Перевіряємо, чи є наступна пачка
            if data.get('exist_next_page'):  # Якщо наступна сторінка є
                next_page_id = data.get('next_page_id')  # Отримуємо next_page_id для наступного запиту
                print(f"🔄 Наступна пачка: {next_page_id}")
            else:
                print("✅ Усі транзакції отримано.")
                break  # Якщо наступної пачки немає, завершуємо цикл
        else:
            print("❌ Помилка отримання транзакцій:", data.get('message'))
            break
    else:
        print(f"❌ Помилка {response.status_code}: {response.text}")
        break

cursor.close()
conn.close()
