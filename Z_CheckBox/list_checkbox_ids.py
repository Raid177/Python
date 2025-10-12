import os
import requests
from dotenv import load_dotenv

load_dotenv()

AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
CASH_REGISTERS_URL = "https://api.checkbox.ua/api/v1/cash-registers"
BRANCHES_URL = "https://api.checkbox.ua/api/v1/branches"

# кого показувати (можеш додавати/забирати)
FOPS = ["DPS_LAV", "DPS_ZVO", "DPS_PMA"]

def safe_results(json_obj):
    if isinstance(json_obj, dict) and "results" in json_obj:
        return json_obj["results"]
    if isinstance(json_obj, list):
        return json_obj
    return []

def fetch(prefix: str):
    license_key = os.getenv(f"{prefix}_LICENSE_KEY")
    pin_code = os.getenv(f"{prefix}_PIN_CODE")
    if not license_key or not pin_code:
        print(f"\n❌ {prefix}: немає LICENSE_KEY або PIN_CODE у .env")
        return

    auth_headers = {
        "accept": "application/json",
        "X-Client-Name": "PetWealth-Integration",
        "X-Client-Version": "1.0",
        "X-License-Key": license_key,
        "Content-Type": "application/json",
    }
    r = requests.post(AUTH_URL, headers=auth_headers, json={"pin_code": pin_code})
    if r.status_code not in (200, 201):
        print(f"\n❌ {prefix}: авторизація не вдалася ({r.status_code}) {r.text}")
        return

    access_token = r.json().get("access_token")
    headers = {"accept": "application/json", "Authorization": f"Bearer {access_token}"}

    cr = requests.get(CASH_REGISTERS_URL, headers=headers)
    br = requests.get(BRANCHES_URL, headers=headers)

    print(f"\n=== {prefix} ===")
    if cr.status_code == 200:
        items = safe_results(cr.json())
        print("✅ Список кас (CASH_REGISTER_ID):")
        for it in items:
            addr = it.get("address") or (it.get("branch") or {}).get("address") or "Без адреси"
            print(f"  - {it['id']} ({addr})")
    else:
        print(f"❌ Помилка отримання кас: {cr.status_code} {cr.text}")

    if br.status_code == 200:
        items = safe_results(br.json())
        print("✅ Список торгових точок (BRANCH_ID):")
        for it in items:
            addr = it.get("address") or "Без адреси"
            print(f"  - {it['id']} ({addr})")
    else:
        print(f"❌ Помилка отримання торгових точок: {br.status_code} {br.text}")

if __name__ == "__main__":
    for f in FOPS:
        fetch(f)
