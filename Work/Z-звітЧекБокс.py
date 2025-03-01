import requests
import time
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 🔹 Завантажуємо змінні з .env
load_dotenv()

# 🔹 Дані для авторизації з .env
CLIENT_NAME = "My-Integration"
CLIENT_VERSION = "1.0"
LICENSE_KEY = os.getenv("DPS_LAV_LICENSE_KEY")
PIN_CODE = os.getenv("DPS_LAV_PIN_CODE")

# 🔹 Ідентифікатори каси та торгової точки з .env
CASH_REGISTER_ID = os.getenv("DPS_LAV_CASH_REGISTER_ID")
BRANCH_ID = os.getenv("DPS_LAV_BRANCH_ID")

# 🔹 Отримання дати за вчора
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
FROM_DATE = f"{yesterday}T00:00:00+0300"
TO_DATE = f"{yesterday}T23:59:59+0300"

# 🔹 URL-адреси API
AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
Z_REPORT_URL = "https://api.checkbox.ua/api/v1/extended-reports/z"
REPORT_STATUS_URL = "https://api.checkbox.ua/api/v1/extended-reports"
REPORT_DOWNLOAD_URL = "https://api.checkbox.ua/api/v1/extended-reports/{report_id}/report.json"

# 🔹 Авторизація (отримуємо токен)
auth_headers = {
    "accept": "application/json",
    "X-Client-Name": CLIENT_NAME,
    "X-Client-Version": CLIENT_VERSION,
    "X-License-Key": LICENSE_KEY,
    "Content-Type": "application/json"
}
auth_data = {"pin_code": PIN_CODE}

auth_response = requests.post(AUTH_URL, headers=auth_headers, json=auth_data)

if auth_response.status_code in [200, 201]:
    ACCESS_TOKEN = auth_response.json().get("access_token")
    print("✅ Авторизація успішна! Отримано токен.\n")
else:
    print(f"❌ Помилка авторизації: {auth_response.status_code}")
    print(auth_response.json())
    exit()

# 🔹 Заголовки для подальших запитів
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

# 🔹 Надсилаємо запит на створення звіту
z_response = requests.post(Z_REPORT_URL, headers=z_headers, json=z_data)

if z_response.status_code == 200:
    report_id = z_response.json().get("id")
    print(f"📊 Запит на формування Z-звіту створено! Report ID: {report_id}\n")
else:
    print(f"❌ Помилка створення звіту: {z_response.status_code}")
    print(z_response.json())
    exit()

# 🔹 Очікуємо готовності звіту (максимум 120 секунд)
print("⏳ Очікуємо готовності звіту...")
for i in range(12):  # 12 спроб, раз на 10 секунд (120 секунд)
    time.sleep(10)
    report_status_response = requests.get(f"{REPORT_STATUS_URL}/{report_id}", headers=z_headers)
    
    if report_status_response.status_code == 429:
        print("⚠️ Занадто часті запити. Чекаємо ще 30 секунд...")
        time.sleep(30)
        continue

    report_status = report_status_response.json()

    if report_status.get("status") == "DONE":
        print("✅ Звіт готовий! Завантажуємо JSON...\n")
        break
    else:
        print(f"⌛ Статус: {report_status.get('status')} (спроба {i+1}/12)")

# 🔹 Отримуємо готовий JSON-звіт
final_report_url = REPORT_DOWNLOAD_URL.format(report_id=report_id)
final_report_response = requests.get(final_report_url, headers=z_headers)

if final_report_response.status_code == 200:
    print("\n✅ Повні дані звіту:")
    print(json.dumps(final_report_response.json(), indent=4, ensure_ascii=False))
else:
    print(f"❌ Помилка отримання JSON-звіту: {final_report_response.status_code}")
    print(final_report_response.json())
