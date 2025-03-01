import requests
import time
import json
import os
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 🔹 Завантажуємо змінні з .env
load_dotenv()

# 🔹 Дані для підключення до БД
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

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

# 🔹 URL-адреси API
AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
Z_REPORT_URL = "https://api.checkbox.ua/api/v1/extended-reports/z"
REPORT_STATUS_URL = "https://api.checkbox.ua/api/v1/extended-reports"
REPORT_DOWNLOAD_URL = "https://api.checkbox.ua/api/v1/extended-reports/{report_id}/report.json"

# 🔹 Функція отримання останньої дати open_date
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
        return (result - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+0300") if result else "2025-01-01T00:00:00+0300"
    except Exception as e:
        print(f"❌ Помилка отримання дати з БД: {e}")
        return "2025-01-01T00:00:00+0300"

# 🔹 Підключення до MySQL
db_conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)
cursor = db_conn.cursor()

# 🔹 SQL-запит для вставки або оновлення
insert_query = """
INSERT INTO Z_CheckBox (
    cash_register_id, branch_id, address, open_date, close_date, receipts_count, 
    fiscal_number, close_fn, shift_serial, turnover, last_receipt, sell_receipts_count, 
    return_receipts_count, vat_amount, tax_rates, tax_names, tax_amount_sale, tax_turnover_sale, 
    tax_amount_return, tax_turnover_return, payment_names, payment_sells, payment_returns, 
    initial_balance, final_balance, service_in, service_out, card_sales, card_returns, cash_sales, cash_returns
) VALUES (
    %(cash_register_id)s, %(branch_id)s, %(address)s, %(open_date)s, %(close_date)s, %(receipts_count)s,
    %(fiscal_number)s, %(close_fn)s, %(shift_serial)s, %(turnover)s, %(last_receipt)s, %(sell_receipts_count)s,
    %(return_receipts_count)s, %(vat_amount)s, %(tax_rates)s, %(tax_names)s, %(tax_amount_sale)s, %(tax_turnover_sale)s,
    %(tax_amount_return)s, %(tax_turnover_return)s, %(payment_names)s, %(payment_sells)s, %(payment_returns)s,
    %(initial_balance)s, %(final_balance)s, %(service_in)s, %(service_out)s, %(card_sales)s, %(card_returns)s, %(cash_sales)s, %(cash_returns)s
) ON DUPLICATE KEY UPDATE 
    updated_at = CURRENT_TIMESTAMP,
    turnover = VALUES(turnover),
    receipts_count = VALUES(receipts_count),
    vat_amount = VALUES(vat_amount);
"""

# 🔹 Обробка всіх кас
for cred in CREDENTIALS:
    print(f"🔑 Отримую токен для {cred['fop']} (Каса: {cred['cash_register_id']}, Точка: {cred['branch_id']})...")

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
    else:
        print(f"❌ Помилка авторизації: {auth_response.status_code}")
        continue

    from_date = get_last_open_date(cred["cash_register_id"], cred["branch_id"])
    to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT23:59:59+0300")

    print(f"📊 Отримую дані за {from_date} - {to_date}...")

    z_headers = {"accept": "application/json", "Authorization": f"Bearer {access_token}"}
    z_data = {"from_date": from_date, "to_date": to_date, "cash_register_id": cred["cash_register_id"], "branch_id": cred["branch_id"], "export_extension": "JSON"}
    z_response = requests.post(Z_REPORT_URL, headers=z_headers, json=z_data)

    if z_response.status_code == 200:
        report_id = z_response.json().get("id")
    else:
        print(f"❌ Помилка створення звіту: {z_response.status_code}")
        continue

    time.sleep(10)
    final_report_response = requests.get(REPORT_DOWNLOAD_URL.format(report_id=report_id), headers=z_headers)

    if final_report_response.status_code == 200:
        z_reports = final_report_response.json()

        for report in z_reports:
            report_data = {**report, "cash_register_id": cred["cash_register_id"], "branch_id": cred["branch_id"]}
            report_data["address"] = report_data["address"].encode("utf-8", "ignore").decode("utf-8")  # ✅ Фікс кодування

            cursor.execute(insert_query, report_data)

    db_conn.commit()
    print(f"✅ Дані для {cred['fop']} вставлені в MySQL!")

cursor.close()
db_conn.close()
print("🚀 Обробка завершена!")
