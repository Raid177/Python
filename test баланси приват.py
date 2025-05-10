import requests

# API ПриватБанку
API_URL = "https://acp.privatbank.ua/api/statements/balance"

# Реквізити для тесту
ACCOUNT = "UA453052990000026004005203890"
DATE = "15-03-2025"  # Вказуємо дату
TOKEN = "3da9b621-1706-4100-94b6-1937f8455a0figKu+L4PGCl0nGd6R1mJYAu4rGaQ+U4JdaCIB9wnMD8anoMVvoAXRRWJdZLOy42URibaY7rxj+3yUj5MXdoG1/DV4UH+WIXbAWir/p7NE/Bcet8gX7y5N8z/bu1Yp8Ct5CS+Pshd9GKyXiYouPm3svxgnOCY80iO+MDWvIeHsT9/mxFRx01M7gB40AMwlv+bXlWor+dECVKz2SbOOfzrymjRj0OqPvFYslVPrQviUuMWKTrIc17jjGVWO8ySuwr1Fw=="  # Підстав свій токен

# Формуємо заголовки (тут важливий `token`, а не `Authorization`)
headers = {
    "User-Agent": "PythonClient",
    "token": TOKEN,
    "Content-Type": "application/json;charset=cp1251"
}

# Формуємо параметри (запит в URL)
params = {
    "acc": ACCOUNT,
    "startDate": DATE,
    "endDate": DATE
}

# Виконуємо запит
response = requests.get(API_URL, headers=headers, params=params)

# Виводимо відповідь API
print(f"Статус-код: {response.status_code}")
print(f"Відповідь API: {response.text}")
