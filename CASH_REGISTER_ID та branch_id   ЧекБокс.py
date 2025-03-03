import requests
import json

# 🔹 Встав свої дані
CLIENT_NAME = "My-Integration"  # Назва інтеграції
CLIENT_VERSION = "1.0"  # Версія інтеграції
LICENSE_KEY = "8def3eaf09b670f04854db92"  # 🔑 Ключ ліцензії каси
PIN_CODE = "0513892725"  # 🔑 Пін-код касира

# 🔹 URL-адреси API
AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
CASH_REGISTERS_URL = "https://api.checkbox.ua/api/v1/cash-registers"
BRANCHES_URL = "https://api.checkbox.ua/api/v1/branches"

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

# 🔹 Заголовки з токеном для наступних запитів
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
}

# 🔹 Отримуємо список кас (CASH_REGISTER_ID)
cash_registers_response = requests.get(CASH_REGISTERS_URL, headers=headers)

if cash_registers_response.status_code == 200:
    cash_registers = cash_registers_response.json()
    print("\n✅ Список кас (CASH_REGISTER_ID):")
    print(json.dumps(cash_registers, indent=4, ensure_ascii=False))
else:
    print(f"❌ Помилка отримання кас: {cash_registers_response.status_code}")
    print(cash_registers_response.json())

# 🔹 Отримуємо список торгових точок (BRANCH_ID)
branches_response = requests.get(BRANCHES_URL, headers=headers)

if branches_response.status_code == 200:
    branches = branches_response.json()
    print("\n✅ Список торгових точок (BRANCH_ID):")
    print(json.dumps(branches, indent=4, ensure_ascii=False))
else:
    print(f"❌ Помилка отримання торгових точок: {branches_response.status_code}")
    print(branches_response.json())
