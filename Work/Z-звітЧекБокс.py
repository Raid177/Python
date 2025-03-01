import requests
import time
import json
import os
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
from datetime import datetime

# 🔹 Завантажуємо змінні з .env
load_dotenv()

# 🔹 Дані для авторизації ЧекБокс
CLIENT_NAME = "My-Integration"
CLIENT_VERSION = "1.0"
LICENSE_KEY = os.getenv("DPS_LAV_LICENSE_KEY")
PIN_CODE = os.getenv("DPS_LAV_PIN_CODE")

# 🔹 Ідентифікатори каси та торгової точки
CASH_REGISTER_ID = os.getenv("DPS_LAV_CASH_REGISTER_ID")
BRANCH_ID = os.getenv("DPS_LAV_BRANCH_ID")

# 🔹 Дані для підключення до БД
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# 🔹 Період (лютий 2025)
FROM_DATE = "2025-02-01T00:00:00+0300"
TO_DATE = "2025-02-28T23:59:59+0300"

# 🔹 URL-адреси API
AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
Z_REPORT_URL = "https://api.checkbox.ua/api/v1/extended-reports/z"
REPORT_STATUS_URL = "https://api.checkbox.ua/api/v1/extended-reports"
REPORT_DOWNLOAD_URL = "https://api.checkbox.ua/api/v1/extended-reports/{report_id}/report.json"

# 🔹 Авторизація в ЧекБокс
auth_headers = {
    "accept": "application/json",
    "X-Client-Name": CLIENT_NAME,
    "X-Client-Version": CLIENT_VERSION,
    "X-License-Key": LICENSE_KEY,
    "Content-Type": "application/json"
}
auth_data = {"pin_code": PIN_CODE}

print(f"🔑 Отримую токен для branch_id: {BRANCH_ID}, cash_register_id: {CASH_REGISTER_ID} за період {FROM_DATE} - {TO_DATE}...")

auth_response = requests.post(AUTH_URL, headers=auth_headers, json=auth_data)

if auth_response.status_code in [200, 201]:
    ACCESS_TOKEN = auth_response.json().get("access_token")
else:
    print(f"❌ Помилка авторизації: {auth_response.status_code}")
    exit()

# 🔹 Формування запиту на отримання звітів
z_headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "X-Client-Name": CLIENT_NAME,
    "X-Client-Version": CLIENT_VERSION,
    "Content-Type": "application/json"
}
z_data = {
    "from_date": FROM_DATE,
    "to_date": TO_DATE,
    "cash_register_id": CASH_REGISTER_ID,
    "branch_id": BRANCH_ID,
    "export_extension": "JSON",
    "organization_info": False
}

print("📊 Запитую дані з ЧекБокс...")

z_response = requests.post(Z_REPORT_URL, headers=z_headers, json=z_data)

if z_response.status_code == 200:
    report_id = z_response.json().get("id")
else:
    print(f"❌ Помилка створення звіту: {z_response.status_code}")
    exit()

# 🔹 Очікування готовності звіту
print("⏳ Очікую даних...")

for _ in range(12):  # Чекаємо 120 секунд (по 10 сек)
    time.sleep(10)
    report_status_response = requests.get(f"{REPORT_STATUS_URL}/{report_id}", headers=z_headers)
    if report_status_response.json().get("status") == "DONE":
        break

# 🔹 Отримання JSON звіту
final_report_url = REPORT_DOWNLOAD_URL.format(report_id=report_id)
final_report_response = requests.get(final_report_url, headers=z_headers)

if final_report_response.status_code == 200:
    z_reports = final_report_response.json()
else:
    print(f"❌ Помилка отримання JSON-звіту: {final_report_response.status_code}")
    exit()

# 🔹 Підключення до MySQL
db_conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)
cursor = db_conn.cursor()

print("📥 Вставляю дані в MySQL...")

# 🔹 SQL-запит для вставки
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
) ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;
"""

# 🔹 Вставка кожного запису
for report in z_reports:
    report_data = {
        "cash_register_id": CASH_REGISTER_ID,
        "branch_id": BRANCH_ID,
        "address": report.get("address"),
        "open_date": report.get("open_date"),
        "close_date": report.get("close_date"),
        "receipts_count": report.get("receipts_count"),
        "fiscal_number": report.get("fiscal_number"),
        "close_fn": report.get("close_fn"),
        "shift_serial": report.get("shift_serial"),
        "turnover": report.get("turnover"),
        "last_receipt": report.get("last_receipt"),
        "sell_receipts_count": report.get("sell_receipts_count"),
        "return_receipts_count": report.get("return_receipts_count"),
        "vat_amount": report.get("vat_amount"),
        "tax_rates": report.get("tax_rates"),
        "tax_names": report.get("tax_names"),
        "tax_amount_sale": report.get("tax_amount_sale"),
        "tax_turnover_sale": report.get("tax_turnover_sale"),
        "tax_amount_return": report.get("tax_amount_return"),
        "tax_turnover_return": report.get("tax_turnover_return"),
        "payment_names": report.get("payment_names"),
        "payment_sells": report.get("payment_sells"),
        "payment_returns": report.get("payment_returns"),
        "initial_balance": report.get("initial_balance"),
        "final_balance": report.get("final_balance"),
        "service_in": report.get("service_in"),
        "service_out": report.get("service_out"),
        "card_sales": report.get("card_sales"),
        "card_returns": report.get("card_returns"),
        "cash_sales": report.get("cash_sales"),
        "cash_returns": report.get("cash_returns")
    }
    
    cursor.execute(insert_query, report_data)

# 🔹 Закриваємо підключення
db_conn.commit()
cursor.close()
db_conn.close()

print("✅ Дані успішно вставлені в MySQL! 🚀")
