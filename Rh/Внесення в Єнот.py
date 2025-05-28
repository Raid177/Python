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
                SELECT question_key, subquestion_key, open_answer 
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
            url_get = f"{ODATA_URL}Document_Анализы(guid'{ref_key_exam}')?$format=json"
            response = requests.get(url_get, auth=(ODATA_USER, ODATA_PASSWORD), headers=HEADERS)
            response.raise_for_status()
            exam_full = response.json()
            log("✅ Завантажено документ з Єнота")

            findings = fetch_findings(ref_key_exam)
            log(f"🧩 Отримано findings: {len(findings)} записів")

            sastav = exam_full.get("Состав", [])
            questions = exam_full.get("hiСписокВопросов", [])
            max_line = max([int(x.get("LineNumber", 0)) for x in sastav] + [0])
            max_cell = max([int(x.get("НомерЯчейки", 0)) for x in sastav] + [0])
            updated_count = 0
            for row in findings:
                qkey = row["question_key"].lower()
                skey = row["subquestion_key"]
                answer = row["open_answer"]

                match = None
                for item in sastav:
                    if item.get("ЭлементарныйВопрос_Key", "").lower() == skey.lower():
                        match = item
                        break

                if match:
                    log(f"🔁 Знайдено існуючий запис для {skey}, оновлюємо ОткрытыйОтвет")
                    match["ОткрытыйОтвет"] = answer
                    updated_count += 1
                    continue

                found_in_hi = next((q for q in questions if q["Вопрос_Key"].lower() == qkey), None)
                if found_in_hi:
                    max_line += 1
                    max_cell += 1
                    new_row = {
                        "Ref_Key": ref_key_exam,
                        "LineNumber": str(max_line),
                        "Вопрос_Key": found_in_hi["Вопрос_Key"],
                        "ЭлементарныйВопрос_Key": skey,
                        "НомерЯчейки": str(max_cell),
                        "Ответ": "",
                        "Ответ_Type": "StandardODATA.Undefined",
                        "ОткрытыйОтвет": answer,
                        "ТипОтвета": "Текст"
                    }
                    sastav.append(new_row)
                    updated_count += 1
                    log(f"➕ Створено новий рядок для {qkey}, LineNumber = {max_line}")
                else:
                    log(f"❌ Не знайдено question_key {qkey} у hiСписокВопросов")

            patch_data = {
                "ДатаРезультата": datetime.now().isoformat(),
                "Комментарий": "🔎 Заключення сформоване автоматично на основі аналізу зображень GPT-4o.",
                "ЕстьРезультаты": True,
                "Состав": sastav
            }

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
