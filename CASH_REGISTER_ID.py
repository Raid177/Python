import requests
import json
import os
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 🔹 Завантажуємо змінні з .env
load_dotenv()

# 🔹 ЧекБокс API: Дані для авторизації
CREDENTIALS = [
    {
        "license_key": os.getenv("DPS_LAV_LICENSE_KEY"),
        "pin_code": os.getenv("DPS_LAV_PIN_CODE"),
        "cash_register_id": os.getenv("DPS_LAV_CASH_REGISTER_ID"),
        "branch_id": os.getenv("DPS_LAV_BRANCH_ID"),
        "fop": "Льодін Олексій Володимирович"
    },
    {
        "license_key": os.getenv("DPS_ZVO_LICENSE_KEY"),
        "pin_code": os.getenv("DPS_ZVO_PIN_CODE"),
        "cash_register_id": os.getenv("DPS_ZVO_CASH_REGISTER_ID"),
        "branch_id": os.getenv("DPS_ZVO_BRANCH_ID"),
        "fop": "Жиліна Валерія Олександрівна"
    }
]

# 🔹 Дані для підключення до БД
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# 🔹 URL-адреси API
AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
CASH_REGISTERS_URL = "https://api.checkbox.ua/api/v1/cash-registers"
BRANCHES_URL = "https://api.checkbox.ua/api/v1/branches"

# 🔹 Функція для отримання останньої дати open_date в БД
def get_last_open_date(cash_register_id, branch_id):
    try:
        db_conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )
        cursor = db_conn.cursor()

        query = """
        SELECT MAX(open_date) FROM Z_CheckBox
        WHERE cash_register_id = %s AND branch_id = %s
        """
        cursor.execute(query, (cash_register_id, branch_id))
        result = cursor.fetchone()[0]

        cursor.close()
        db_conn.close()

        if result:
            last_date = result - timedelta(days=1)
        else:
            last_date = datetime(2025, 1, 1)  # Якщо БД пуста, беремо 1 січня 2025

        return last_date.strftime("%Y-%m-%dT00:00:00+0300")

    except Exception as e:
        print(f"❌ Помилка отримання дати з БД: {e}")
        return "2025-01-01T00:00:00+0300"  # Безпечне значення

# 🔹 Отримання списку кас і торгових точок
for cred in CREDENTIALS:
    print(f"🔑 Авторизуюсь для ФОП {cred['fop']} (Каса: {cred['cash_register_id']}, Точка: {cred['branch_id']})...")

    # 🔹 Авторизація в ЧекБокс
    auth_headers = {
        "accept": "application/json",
        "X-Client-Name": "My-Integration",
        "X-Client-Version": "1.0",
        "X-License-Key": cred["license_key"],
        "Content-Type": "application/json"
    }
    auth_data = {"pin_code": cred["pin_code"]}

    auth_response = requests.post(AUTH_URL, headers=auth_headers, json=auth_data)

    if auth_response.status_code in [200, 201]:
        access_token = auth_response.json().get("access_token")
        print(f"✅ Авторизація успішна для {cred['fop']}")
    else:
        print(f"❌ Помилка авторизації: {auth_response.status_code}")
        continue

    # 🔹 Отримуємо період для каси
    from_date = get_last_open_date(cred["cash_register_id"], cred["branch_id"])
    to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT23:59:59+0300")

    print(f"📊 Отримую дані за період {from_date} - {to_date}...")

    # 🔹 Запитуємо список кас
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    cash_registers_response = requests.get(CASH_REGISTERS_URL, headers=headers)
    branches_response = requests.get(BRANCHES_URL, headers=headers)

    # 🔹 Виводимо каси
    if cash_registers_response.status_code == 200:
        cash_registers = cash_registers_response.json()
        print(f"\n✅ Список кас для {cred['fop']} (CASH_REGISTER_ID):")
        for register in cash_registers.get("results", []):
            print(f"   - {register['id']} ({register.get('address', 'Без адреси')})")
    else:
        print(f"❌ Помилка отримання кас: {cash_registers_response.status_code}")

    # 🔹 Виводимо торгові точки
    if branches_response.status_code == 200:
        branches = branches_response.json()
        print(f"\n✅ Список торгових точок для {cred['fop']} (BRANCH_ID):")
        for branch in branches.get("results", []):
            print(f"   - {branch['id']} ({branch.get('address', 'Без адреси')})")
    else:
        print(f"❌ Помилка отримання торгових точок: {branches_response.status_code}")

print("\n🚀 Обробка завершена!")
