import requests
from dotenv import load_dotenv
import os
import mysql.connector
from datetime import datetime, timedelta

# Завантажуємо змінні з .env файлу
load_dotenv()

# Номери рахунків для кожного ФОП
accounts_fop1 = ['UA973052990000026002025035545']
accounts_fop2 = ['UA173375460000026000045200003']

# Токени для кожного ФОП
tokens = {
    'FOP1': os.getenv('API_TOKEN_LOV'),
    'FOP2': os.getenv('API_TOKEN_ZVO'),
}

# Підключення до БД
conn = mysql.connector.connect(host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'), database=os.getenv('DB_DATABASE'))
cursor = conn.cursor()

# Функція для обробки транзакцій
def fetch_and_save_transactions(account_number, token, start_date, end_date):
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
                            # Перетворення дат
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
                        
                        # Збереження транзакції в БД
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
                    print(f"✅ Пачка з {len(transactions)} транзакцій для рахунку {account_number} успішно збережена у БД")
                else:
                    print(f"❌ Немає транзакцій для рахунку {account_number}")
                
                # Перевірка на наявність наступної пачки
                if data.get('exist_next_page'):
                    next_page_id = data.get('next_page_id')
                    print(f"🔄 Наступна пачка для рахунку {account_number}: {next_page_id}")
                else:
                    print(f"✅ Усі транзакції для рахунку {account_number} отримано.")
                    break
            else:
                print(f"❌ Помилка отримання транзакцій для рахунку {account_number}: {data.get('message')}")
                break
        else:
            print(f"❌ Помилка {response.status_code} для рахунку {account_number}: {response.text}")
            break

# Перебір рахунків для кожного ФОП
for fop, token in tokens.items():
    print(f"🔑 Використовується токен для {fop}")
    for account in (accounts_fop1 if fop == 'FOP1' else accounts_fop2):
        print(f"📅 Одержання транзакцій для рахунку {account}")
        
        # Отримуємо максимальну дату для конкретного рахунку
        cursor.execute("""
            SELECT MAX(DATE_TIME_DAT_OD_TIM_P)
            FROM bnk_trazact_prvt
            WHERE AUT_MY_ACC = %s
        """, (account,))
        last_date = cursor.fetchone()[0]

        print(f"🔍 Рахунок: {account}, Знайдена дата: {last_date}")

        # Визначаємо дату старту
        if last_date:
            start_date = (last_date - timedelta(days=1)).strftime('%d-%m-%Y')
        else:
            # Якщо не знайдена дата, встановлюємо стартову дату на 01.07.2024
            start_date = '01-07-2024'

        # Дата фінішу - поточна дата у форматі 'dd-MM-yyyy'
        end_date = datetime.now().strftime('%d-%m-%Y')

        print(f"📅 Дата старту для рахунку {account}: {start_date}")
        print(f"📅 Дата фінішу для рахунку {account}: {end_date}")



        # Викликаємо функцію для отримання та збереження транзакцій
        fetch_and_save_transactions(account, token, start_date, end_date)

# Закриваємо з'єднання з БД
cursor.close()
conn.close()
