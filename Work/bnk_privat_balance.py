import os
import time
import requests
import pymysql
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Завантажуємо змінні оточення
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
accounts = {
    'FOP1': ['UA973052990000026002025035545'],  # ЛАВ
    'FOP2': ['UA173375460000026000045200003', 'UA453052990000026004005203890']  # ЖВА
}

# Токени для кожного ФОП
tokens = {
    'FOP1': os.getenv('API_TOKEN_LOV'),
    'FOP2': os.getenv('API_TOKEN_ZVO')
}

# URL API для отримання балансів
API_URL = "https://acp.privatbank.ua/api/statements/balance"

# Отримання останньої дати балансу для рахунку
def get_last_balance_date(account):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            query = "SELECT MAX(balance_date) AS last_date FROM bnk_privat_balance WHERE acc = %s"
            cursor.execute(query, (account,))
            result = cursor.fetchone()
            return result["last_date"].date() if result["last_date"] else None  # Приводимо до date
    finally:
        connection.close()

# Збереження балансів у БД
def save_balances_to_db(balances):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            insert_query = """
                INSERT INTO bnk_privat_balance (
                    acc, balance_date, currency, balanceIn, balanceInEq, balanceOut, balanceOutEq, 
                    turnoverDebt, turnoverDebtEq, turnoverCred, turnoverCredEq, dpd, 
                    nameACC, date_open_acc_reg, date_open_acc_sys, date_close_acc, is_final_bal
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    balanceIn = VALUES(balanceIn),
                    balanceInEq = VALUES(balanceInEq),
                    balanceOut = VALUES(balanceOut),
                    balanceOutEq = VALUES(balanceOutEq),
                    turnoverDebt = VALUES(turnoverDebt),
                    turnoverDebtEq = VALUES(turnoverDebtEq),
                    turnoverCred = VALUES(turnoverCred),
                    turnoverCredEq = VALUES(turnoverCredEq),
                    is_final_bal = VALUES(is_final_bal),
                    updated_at = CURRENT_TIMESTAMP
            """
            for bal in balances:
                cursor.execute(insert_query, (
                    bal["acc"], bal["balance_date"], bal["currency"], bal["balanceIn"], bal["balanceInEq"],
                    bal["balanceOut"], bal["balanceOutEq"], bal["turnoverDebt"], bal["turnoverDebtEq"],
                    bal["turnoverCred"], bal["turnoverCredEq"], bal["dpd"], bal["nameACC"],
                    bal["date_open_acc_reg"], bal["date_open_acc_sys"], bal["date_close_acc"],
                    bal["is_final_bal"]
                ))
        connection.commit()
    finally:
        connection.close()

# Отримання балансів для конкретного рахунку за конкретну дату
def get_balances(account, date, token):
    headers = {
        "User-Agent": "PythonClient",
        "token": token,
        "Content-Type": "application/json;charset=cp1251"
    }
    params = {
        "acc": account,
        "startDate": date.strftime("%d-%m-%Y"),
        "endDate": date.strftime("%d-%m-%Y")
    }
    
    response = requests.get(API_URL, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"❌ Помилка запиту: {response.status_code}, {response.text}")
        return []

    data = response.json()
    if data["status"] != "SUCCESS" or "balances" not in data:
        print(f"⚠️ Немає даних у відповіді API: {data}")
        return []

    return [
        {
            "acc": bal["acc"],
            "balance_date": date,
            "currency": bal["currency"],
            "balanceIn": bal["balanceIn"],
            "balanceInEq": bal["balanceInEq"],
            "balanceOut": bal["balanceOut"],
            "balanceOutEq": bal["balanceOutEq"],
            "turnoverDebt": bal["turnoverDebt"],
            "turnoverDebtEq": bal["turnoverDebtEq"],
            "turnoverCred": bal["turnoverCred"],
            "turnoverCredEq": bal["turnoverCredEq"],
            "dpd": datetime.strptime(bal["dpd"], "%d.%m.%Y %H:%M:%S").date() if bal["dpd"] else None,
            "nameACC": bal["nameACC"],
            "date_open_acc_reg": datetime.strptime(bal["date_open_acc_reg"], "%d.%m.%Y %H:%M:%S").date() if bal["date_open_acc_reg"] else None,
            "date_open_acc_sys": datetime.strptime(bal["date_open_acc_sys"], "%d.%m.%Y %H:%M:%S").date() if bal["date_open_acc_sys"] else None,
            "date_close_acc": datetime.strptime(bal["date_close_acc"], "%d.%m.%Y %H:%M:%S").date() if bal["date_close_acc"] else None,
            "is_final_bal": bal["is_final_bal"]
        } for bal in data["balances"]
    ]

# Основна логіка
def main():
    start_time = datetime.now()
    total_records = 0

    for fop, acc_list in accounts.items():
        token = tokens[fop]

        for account in acc_list:
            # Отримуємо останню дату балансу або починаємо з 01.07.2024
            last_date = get_last_balance_date(account)
            if last_date:
                start_date = last_date - timedelta(days=1)  # Мінус 1 день
            else:
                start_date = datetime(2024, 7, 1).date()  # Приводимо до date

            end_date = (datetime.now() - timedelta(days=1)).date()  # Вчорашній день

            print(f"📌 Отримання балансів для рахунку {account} з {start_date.strftime('%d-%m-%Y')} по {end_date.strftime('%d-%m-%Y')}")

            current_date = start_date
            while current_date <= end_date:
                balances = get_balances(account, current_date, token)
                if balances:
                    save_balances_to_db(balances)
                    total_records += len(balances)
                    print(f"✅ Рахунок {account} - отримано баланс за {current_date.strftime('%d-%m-%Y')} (записів: {len(balances)})")
                else:
                    print(f"⚠️ Рахунок {account} - немає даних за {current_date.strftime('%d-%m-%Y')}")

                current_date += timedelta(days=1)
                time.sleep(1)  # Пауза 1 секунда

    end_time = datetime.now()
    print(f"🎯 Завершено! Усього отримано {total_records} записів за {end_time - start_time}.")

if __name__ == "__main__":
    main()
