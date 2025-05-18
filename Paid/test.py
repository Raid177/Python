import requests
from datetime import datetime
from dotenv import dotenv_values

# === Завантаження змінних оточення
env = dotenv_values("/root/Python/.env")

ODATA_URL = env["ODATA_URL"]
ODATA_USER = env["ODATA_USER"]
ODATA_PASSWORD = env["ODATA_PASSWORD"]

ODATA_ACCOUNTS = {
    "Інкассація (транзитний)": "7e87f26e-eaad-11ef-9d9b-2ae983d8a0f0",
    "Реєстратура каса": "a7dda748-86d1-11ef-839c-2ae983d8a0f0",
    "Каса Організації": "f179f3be-4e84-11ef-83bb-2ae983d8a0f0"
}

now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

print("🔍 Перевірка залишків OData:\n")

for name, key in ODATA_ACCOUNTS.items():
    url = (
        f"{ODATA_URL}AccumulationRegister_ДенежныеСредства/Balance"
        f"?Period=datetime'{now_iso}'"
        f"&$format=json"
        f"&Condition=ДенежныйСчет_Key eq guid'{key}'"
    )

    print(f"➡️ {name}")
    print(f"🔗 Запит: {url}")
    try:
        r = requests.get(url, auth=(ODATA_USER, ODATA_PASSWORD))
        print(f"Status: {r.status_code}")
        r.raise_for_status()
        data = r.json()
        rows = data.get("value", [])
        if rows:
            amount = float(rows[0].get("СуммаBalance", 0))
            print(f"✅ Залишок: {amount:,.2f} грн")
        else:
            print("⚠️ Відповідь пуста")
    except Exception as e:
        print(f"❌ Помилка: {e}")
    print("-" * 60)
