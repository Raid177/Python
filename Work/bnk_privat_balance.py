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

# Отримання останньої дати балансу та дати закриття рахунку
def get_last_balance_info(account):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT MAX(balance_date) AS last_date, MAX(date_close_acc) AS close_date
                FROM bnk_privat_balance WHERE acc = %s
            """
            cursor.execute(query, (account,))
            result = cursor.fetchone()
            
            last_date = result["last_date"] if result["last_date"] else None
            close_date = result["close_date"] if result["close_date"] and result["close_date"] != datetime(1900, 1, 1) else None
            
            # Приводимо close_date до date, якщо воно у форматі datetime
            if close_date and isinstance(close_date, datetime):
                close_date = close_date.date()

            return last_date, close_date
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
                    date_close_acc = VALUES(date_close_acc),
                    is_final_bal = VALUES(is_final_bal),
                    updated_at = CURRENT_TIMESTAMP
            """
            for bal in balances:
                date_close_acc = bal["date_close_acc"]
                if date_close_acc and date_close_acc == datetime(1900, 1, 1).date():
                    date_close_acc = None  # Замінюємо 1900-01-01 на NULL

                cursor.execute(insert_query, (
                    bal["acc"], bal["balance_date"], bal["currency"], bal["balanceIn"], bal["balanceInEq"],
                    bal["balanceOut"], bal["balanceOutEq"], bal["turnoverDebt"], bal["turnoverDebtEq"],
                    bal["turnoverCred"], bal["turnoverCredEq"], bal["dpd"], bal["nameACC"],
                    bal["date_open_acc_reg"], bal["date_open_acc_sys"], date_close_acc, bal["is_final_bal"]
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
            last_date, close_date = get_last_balance_info(account)
            
            if close_date and last_date and close_date <= last_date:
                print(f"🔴 Рахунок {account} закритий {close_date.strftime('%d-%m-%Y')}, пропускаємо.")
                continue

            start_date = (last_date - timedelta(days=1)) if last_date else datetime(2024, 7, 1).date()
            end_date = (datetime.now() - timedelta(days=1)).date()

            print(f"📌 Отримання балансів для рахунку {account} з {start_date.strftime('%d-%m-%Y')} по {end_date.strftime('%d-%m-%Y')}")

            current_date = start_date
            while current_date <= end_date:
                balances = get_balances(account, current_date, token)
                if balances:
                    save_balances_to_db(balances)
                    total_records += len(balances)
                    print(f"✅ Рахунок {account} - отримано баланс за {current_date.strftime('%d-%m-%Y')}")
                else:
                    print(f"⚠️ Рахунок {account} - немає даних за {current_date.strftime('%d-%m-%Y')}")

                current_date += timedelta(days=1)
                time.sleep(1)

    print(f"🎯 Завершено! Отримано {total_records} записів за {datetime.now() - start_time}.")

if __name__ == "__main__":
    main()
