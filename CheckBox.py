import requests
import base64

# ⚠️ Вкажіть правильний шлях до файлу КЕП (.jks або .dat)
kep_file_path = r"C:\Users\la\OneDrive\Pet Wealth\ФОПи\Ключи\КЕП\Льодін\pb_2823811772.jks"

# ⚠️ Вкажіть правильний пароль для КЕП
kep_password = "TvzS5687pJ"

# Читаємо КЕП у base64
with open(kep_file_path, "rb") as file:
    kep_base64 = base64.b64encode(file.read()).decode()

# URL для авторизації
auth_url = "https://api.checkbox.ua/api/v1/cashier/signin"

# Формуємо правильний запит
payload = {
    "key": kep_base64,  # Кодований КЕП
    "password": kep_password  # Пароль до ключа
}

# Відправляємо запит
response = requests.post(auth_url, json=payload)

# Виводимо результат
print("Статус-код:", response.status_code)
print("Відповідь:", response.json())
