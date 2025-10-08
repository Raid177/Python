import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# ---------- CONFIG ----------
load_dotenv("/root/Python/.env")

PB_TOKENS = {
    "LOV": os.getenv("API_TOKEN_LOV"),
    "ZVO": os.getenv("API_TOKEN_ZVO"),
    "PMA": os.getenv("API_TOKEN_PMA"),
}

PB_ACCOUNTS = {
    "LOV": [os.getenv("API_АСС_LOV")],
    "ZVO": [acc.strip() for acc in os.getenv("API_АСС_ZVO", "").split(",") if acc.strip()],
    "PMA": [os.getenv("API_АСС_PMA")],
}

# ---------- MAIN ----------
today = datetime.now().strftime("%d-%m-%Y")
url = "https://acp.privatbank.ua/api/statements/balance"

total = 0.0

for name, token in PB_TOKENS.items():
    if not token:
        print(f"⚠️  {name}: токен відсутній у .env")
        continue
    print(f"\n{name} ({len(PB_ACCOUNTS.get(name, []))} рахунки):")

    for acc in PB_ACCOUNTS.get(name, []):
        try:
            headers = {
                "User-Agent": "PythonClient",
                "token": token,
                "Content-Type": "application/json;charset=cp1251"
            }
            params = {"acc": acc, "startDate": today, "endDate": today}
            r = requests.get(url, headers=headers, params=params, timeout=(5, 20))
            r.raise_for_status()

            data = r.json()
            balances = data.get("balances", [])
            if not balances:
                print(f"   ❌ Немає даних для {acc}")
                continue

            for bal in balances:
                bal_name = bal.get("nameACC", acc)
                amount = float(bal.get("balanceOutEq", 0))
                print(f"   💰 {bal_name}: {amount:,.2f} грн")
                total += amount

        except Exception as e:
            print(f"   💥 Помилка для {acc}: {e}")

print(f"\n📊 Загальний баланс усіх рахунків: {total:,.2f} грн")
