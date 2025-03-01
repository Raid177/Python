import requests
import time
import json
from datetime import datetime, timedelta

# 🔹 Дані для авторизації
CLIENT_NAME = "My-Integration"  # Назва інтеграції
CLIENT_VERSION = "1.0"  # Версія інтеграції
LICENSE_KEY = "4ca2a0429412a537b1eb6b1b"  # 🔑 Ключ ліцензії каси
PIN_CODE = "9334687350"  # 🔑 Пін-код касира

# 🔹 Отримання дати за вчора
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
FROM_DATE = f"{yesterday}T00:00:00+0300"
TO_DATE = f"{yesterday}T23:59:59+0300"

# 🔹 URL-адреси API
AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
Z_REPORT_URL = "https://api.checkbox.ua/api/v1/extended-reports/z"
REPORT_STATUS_URL = "https://api.checkbox.ua/api/v1/extended-reports"  # /{report_id}

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

# 🔹 Унікальні ідентифікатори каси та магазину
CASH_REGISTER_ID = "c13876dd-e51e-433f-a61c-f2c426311111"
BRANCH_ID = "b57123f9-8e9a-49c8-a3f2-3c04d3d11111"

# 🔹 Запит Z-звіту
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

# 🔹 Очікуємо, поки звіт сформується (максимум 60 секунд)
print("⏳ Очікуємо готовності звіту...")
for i in range(12):  # Повторюємо запит 12 разів (раз на 5 секунд)
    time.sleep(5)
    report_status_response = requests.get(f"{REPORT_STATUS_URL}/{report_id}", headers=z_headers)
    report_status = report_status_response.json()

    if report_status.get("status") == "DONE":
        print("✅ Звіт готовий! Отримуємо дані...\n")
        break
    else:
        print(f"⌛ Статус: {report_status.get('status')} (спроба {i+1}/12)")

# 🔹 Отримуємо готовий звіт
final_report_response = requests.get(f"{REPORT_STATUS_URL}/{report_id}", headers=z_headers)

if final_report_response.status_code == 200:
    print("\n✅ Повні дані звіту:")
    print(json.dumps(final_report_response.json(), indent=4, ensure_ascii=False))
else:
    print(f"❌ Помилка отримання готового звіту: {final_report_response.status_code}")
    print(final_report_response.json())
