import os
import requests
from dotenv import dotenv_values
from datetime import datetime

# Завантаження .env
env = dotenv_values("/root/Python/.env")

PB_TOKENS = {
    "LOV": env.get("API_TOKEN_LOV"),
    "ZVO": env.get("API_TOKEN_ZVO")
}
PB_ACCOUNTS = {
    "LOV": [env.get("API_АСС_LOV", "")],
    "ZVO": env.get("API_АСС_ZVO", "").split(",")
}

API_URL = "https://acp.privatbank.ua/api/statements/balance"

# Дата запиту — сьогодні
today = datetime.now().strftime("%d-%m-%Y")

print("🔍 Тест ACP-балансу ПриватБанку\n")

for name, token in PB_TOKENS.items():
    for acc in PB_ACCOUNTS.get(name, []):
        print(f"➡️ {name}: {acc}")
        headers = {
            "User-Agent": "PythonClient",
            "token": token,
            "Content-Type": "application/json;charset=cp1251"
        }
        params = {
            "acc": acc,
            "startDate": today,
            "endDate": today
        }

        try:
            response = requests.get(API_URL, headers=headers, params=params)
            print(f"Status: {response.status_code}")
            if response.status_code != 200:
                print(f"❌ Помилка: {response.text}\n")
                continue

            data = response.json()
            print("📦 JSON keys:", data.keys())
            print("🧾 status:", data.get("status"))

            balances = data.get("balances", [])
            if not balances:
                print("⚠️ Немає даних balances")
                continue

            for bal in balances:
                print("🔹 ПОЛЯ:")
                for k, v in bal.items():
                    print(f"{k}: {v}")
                print("-" * 40)

        except Exception as e:
            print(f"❌ Виняток: {e}")

        print("=" * 60)
