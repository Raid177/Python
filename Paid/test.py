import requests
from dotenv import dotenv_values
from datetime import datetime

env = dotenv_values("/root/Python/.env")

API_URL = "https://acp.privatbank.ua/api/statements/balance"

def get_balance(acc, token, date=None):
    if date is None:
        date = datetime.now().date()
    headers = {
        "User-Agent": "PythonClient",
        "token": token,
        "Content-Type": "application/json;charset=cp1251"
    }
    params = {
        "acc": acc,
        "startDate": date.strftime("%d-%m-%Y"),
        "endDate": date.strftime("%d-%m-%Y")
    }
    try:
        response = requests.get(API_URL, headers=headers, params=params)
        print(f"\n=== Запит на {acc} ({date}) ===")
        print(f"Код відповіді: {response.status_code}")
        print(f"Відповідь:\n{response.text}")
    except Exception as e:
        print(f"❗ Виняток: {e}")

# Зчитування токенів та рахунків з env
accounts = []
for var in env:
    if var.startswith("API_TOKEN_"):
        fop = var.replace("API_TOKEN_", "")
        token = env[var]
        acc_list = env.get(f"API_АСС_" + fop, "").split(",")
        for acc in acc_list:
            acc = acc.strip()
            if acc:
                accounts.append((fop, acc, token))

# Тестовий запуск
for fop, acc, token in accounts:
    print(f"\n>>> Перевіряємо {fop} — {acc}")
    get_balance(acc, token)
