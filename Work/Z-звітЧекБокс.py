import requests
import time
import os
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 🔹 Завантаження змінних з .env
load_dotenv()

# 🔹 Дані для підключення до БД
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# 🔹 Дані по ФОПах
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

AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
Z_REPORT_URL = "https://api.checkbox.ua/api/v1/extended-reports/z"
REPORT_DOWNLOAD_URL = "https://api.checkbox.ua/api/v1/extended-reports/{report_id}/report.json"

# 🔹 Підключення до MySQL
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)
cursor = conn.cursor()

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

# 🔹 Отримати останню дату open_date для каси
get_last_open_date_query = """
SELECT MAX(open_date) FROM Z_CheckBox
WHERE cash_register_id = %s AND branch_id = %s
"""

for cred in CREDENTIALS:
    print(f"[AUTH] Авторизуюсь для {cred['fop']}...")

    auth_headers = {
        "accept": "application/json",
        "X-Client-Name": "My-Integration",
        "X-Client-Version": "1.0",
        "X-License-Key": cred["license_key"],
        "Content-Type": "application/json"
    }
    auth_data = {"pin_code": cred["pin_code"]}
    auth_response = requests.post(AUTH_URL, headers=auth_headers, json=auth_data)

    if auth_response.status_code not in [200, 201]:
        print(f"[ERROR] Авторизація не вдалася: {auth_response.status_code}")
        continue

    access_token = auth_response.json().get("access_token")
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "X-Client-Name": "My-Integration",
        "X-Client-Version": "1.0",
        "Content-Type": "application/json"
    }

    # 🔹 Визначаємо стартовий період для каси
    cursor.execute(get_last_open_date_query, (cred["cash_register_id"], cred["branch_id"]))
    last_date = cursor.fetchone()[0]
    start_date = (last_date - timedelta(days=1)) if last_date else datetime(2025, 1, 1)
    end_date = datetime.now() - timedelta(days=1)

    # 🔹 Формування періодів лише до вчора
    PERIODS = []
    start = start_date
    while start <= end_date:
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        if end > end_date:
            end = end_date
        PERIODS.append((start, end))
        start = end + timedelta(days=1)

    for period_start, period_end in PERIODS:
        from_date = period_start.strftime("%Y-%m-%dT00:00:00+0300")
        to_date = period_end.strftime("%Y-%m-%dT23:59:59+0300")

        print(f"[INFO] Запитую {from_date} - {to_date}...")
        z_data = {
            "from_date": from_date,
            "to_date": to_date,
            "cash_register_id": cred["cash_register_id"],
            "branch_id": cred["branch_id"],
            "export_extension": "JSON"
        }

        attempt = 0
        while attempt < 3:
            z_response = requests.post(Z_REPORT_URL, headers=headers, json=z_data)

            if z_response.status_code == 200:
                report_id = z_response.json().get("id")
                time.sleep(10)
                report_response = requests.get(REPORT_DOWNLOAD_URL.format(report_id=report_id), headers=headers)

                if report_response.status_code == 200:
                    z_reports = report_response.json()
                    print(f"[SUCCESS] Отримано {len(z_reports)} звітів для {cred['fop']} за {from_date[:10]} - {to_date[:10]}")

                    for report in z_reports:
                        report_data = {
                            **report,
                            "cash_register_id": cred["cash_register_id"],
                            "branch_id": cred["branch_id"],
                            "address": report.get("address", "").encode("utf-8", "ignore").decode("utf-8")
                        }
                        cursor.execute(insert_query, report_data)
                    conn.commit()
                    time.sleep(30)  # анти-флуд пауза
                    break
                else:
                    print(f"[ERROR] Помилка отримання звіту: {report_response.status_code}")
                    break

            elif z_response.status_code == 429:
                print("[WARN] Ліміт запитів перевищено, чекаю 60 секунд...")
                time.sleep(60)
                attempt += 1
            else:
                print(f"[ERROR] Помилка створення звіту: {z_response.status_code}")
                break

cursor.close()
conn.close()
print("\n[FINISH] Обробка завершена!")
