import requests

# 🔹 Вставляємо свої реальні дані тут:
CLIENT_NAME = "My-Integration"  # Назва інтеграції
CLIENT_VERSION = "1.0"  # Версія інтеграції
LICENSE_KEY = "4ca2a0429412a537b1eb6b1b"  # 🔑 Ключ ліцензії каси
PIN_CODE = "9334687350"  # 🔑 Пін-код касира

# 🔹 URL API для авторизації
CHECKBOX_API_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"

# 🔹 Заголовки запиту
headers = {
    "accept": "application/json",
    "X-Client-Name": CLIENT_NAME,
    "X-Client-Version": CLIENT_VERSION,
    "X-License-Key": LICENSE_KEY,
    "Content-Type": "application/json"
}

# 🔹 Тіло запиту
data = {
    "pin_code": PIN_CODE
}

# 🔹 Виконання POST-запиту
response = requests.post(CHECKBOX_API_URL, headers=headers, json=data)

# 🔹 Обробка відповіді
if response.status_code in [200, 201]:  # Обробка успішної відповіді
    response_json = response.json()
    token = response_json.get("access_token")
    if token:
        print(f"✅ Авторизація успішна!\n🔑 Отриманий токен:\n{token}")
    else:
        print(f"⚠️ Авторизація пройшла, але токен відсутній у відповіді:\n{response_json}")
else:
    print(f"❌ Помилка авторизації: {response.status_code}")
    print(response.json())  # Виведення детальної інформації про помилку
