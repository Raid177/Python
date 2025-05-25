import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# === Налаштування авторизації ===
ODATA_USER = "odata"
ODATA_PASSWORD = "zX8a7M36yU"

def analyze_cast_fields(record: dict):
    results = []
    for key in record:
        if key.endswith("_Type"):
            base_field = key.replace("_Type", "")
            results.append({
                "Поле": base_field,
                "Тип": "Reference (універсальне)",
                "CAST потрібен?": "✅ ТАК"
            })
        elif key.endswith("_Key"):
            results.append({
                "Поле": key,
                "Тип": "Пряме посилання (тип відомий)",
                "CAST потрібен?": "❌ НІ"
            })
        else:
            results.append({
                "Поле": key,
                "Тип": "Не є посиланням",
                "CAST потрібен?": "❌ НІ"
            })
    return pd.DataFrame(results)

def ensure_format_and_top(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    # додаємо параметри, якщо їх немає
    query.setdefault("$format", ["json"])
    query.setdefault("$top", ["1"])

    new_query = urlencode(query, doseq=True)
    updated_url = urlunparse(parsed._replace(query=new_query))
    return updated_url

def fetch_and_analyze(url: str):
    final_url = ensure_format_and_top(url)
    print(f"\n📡 Виконуємо запит:\n{final_url}\n")
    response = requests.get(final_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD))

    try:
        data = response.json()
    except Exception as e:
        print("❌ Помилка при розпарсуванні відповіді:")
        print(response.text)
        return

    if "value" not in data or not data["value"]:
        print("⚠️ Немає записів у відповіді.")
        return

    first_record = data["value"][0]
    print("✅ Перший запис отримано. Аналізую поля...\n")

    df = analyze_cast_fields(first_record)

    # без tabulate — fallback
    try:
        print(df.to_markdown(index=False))
    except ImportError:
        print(df.to_string(index=False))

# === Старт ===
if __name__ == "__main__":
    test_url = input("Встав OData URL (без $top та $format):\n> ").strip()
    fetch_and_analyze(test_url)
