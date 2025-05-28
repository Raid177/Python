# === update_analysis_from_findings.py ===

import requests
import pymysql
import json
from datetime import datetime
from dotenv import dotenv_values

# === 🌱 Завантаження .env змінних ===
env = dotenv_values("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

ODATA_URL = env["ODATA_URL_COPY"]
ODATA_USER = env["ODATA_USER"]
ODATA_PASSWORD = env["ODATA_PASSWORD"]

DB_HOST = env["DB_HOST"]
DB_USER = env["DB_USER"]
DB_PASSWORD = env["DB_PASSWORD"]
DB_DATABASE = env["DB_DATABASE"]

HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

# === 🔌 Підключення до БД ===
def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        cursorclass=pymysql.cursors.DictCursor
    )

# === 📅 Отримати запити зі статусом 'parsed' ===
def fetch_parsed_requests():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM bot_study_requests WHERE status = 'parsed'")
            return cursor.fetchall()

# === 📅 Отримати відповіді GPT для дослідження ===
def fetch_findings(ref_key_exam):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT subquestion_key, open_answer 
                FROM bot_study_findings 
                WHERE Ref_KeyEXAM = %s
            """, (ref_key_exam,))
            return cursor.fetchall()

# === ⚒️ Оновити статус дослідження в БД ===
def update_request_status(ref_key_exam):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE bot_study_requests SET status = 'done', updated_at = NOW() 
                WHERE Ref_KeyEXAM = %s
            """, (ref_key_exam,))
            conn.commit()

# === 📋 Логування подій ===
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# === 🚀 Основна логіка ===
def main():
    parsed_requests = fetch_parsed_requests()

    for req in parsed_requests:
        ref_key_exam = req["Ref_KeyEXAM"]
        log(f"🔎 Обробка дослідження {ref_key_exam}")

        try:
            # 1. Отримати документ з Єнота
            url_get = f"{ODATA_URL}Document_Анализы(guid'{ref_key_exam}')?$format=json"
            response = requests.get(url_get, auth=(ODATA_USER, ODATA_PASSWORD), headers=HEADERS)
            response.raise_for_status()
            exam_full = response.json()
            log("✅ Завантажено документ з Єнота")

            # 2. Отримати мапу відповідей (нижній регістр ключів)
            findings = fetch_findings(ref_key_exam)
            finding_map = {f['subquestion_key'].lower(): f['open_answer'] for f in findings}
            log(f"🧩 Отримано findings ({len(findings)}): {list(finding_map.keys())}")

            # 3. Оновити лише ОткрытыйОтвет у відповідних полях
            updated_count = 0
            for item in exam_full.get("Состав", []):
                key = item.get("ЭлементарныйВопрос_Key")
                if key:
                    key_lower = key.lower()
                    keys_list = list(finding_map.keys())
                    log(f"🔍 Перевірка {key} -> {key_lower} серед {keys_list} => {'✅ match' if key_lower in finding_map else '❌ no match'}")
                    if key_lower in finding_map:
                        item["ОткрытыйОтвет"] = finding_map[key_lower]
                        updated_count += 1
                        log(f"✍️ Оновлено ОткрытыйОтвет для ЭлементарныйВопрос_Key = {key}")
                    else:
                        log(f"🔎 Пропущено — відсутній у findings: {key}")

            # 4. Підготувати PATCH-тіло з повним складом
            patch_data = {
                "ДатаРезультата": datetime.now().isoformat(),
                "Комментарий": "🔎 Заключення сформоване автоматично на основі аналізу зображень GPT-4o.",
                "ЕстьРезультаты": True,
                "Состав": exam_full.get("Состав", [])
            }

            # 5. Надіслати PATCH-запит
            url_patch = f"{ODATA_URL}Document_Анализы(guid'{ref_key_exam}')"
            patch_response = requests.patch(url_patch, auth=(ODATA_USER, ODATA_PASSWORD), headers=HEADERS, json=patch_data)
            patch_response.raise_for_status()

            log(f"⬆️ Внесено оновлення в Єнот ({updated_count} відповідей)")

            update_request_status(ref_key_exam)
            log("🔄 Статус оновлено на 'done' у bot_study_requests\n")

        except Exception as e:
            log(f"❌ ПОМИЛКА для {ref_key_exam}: {e}\n")

if __name__ == "__main__":
    main()
