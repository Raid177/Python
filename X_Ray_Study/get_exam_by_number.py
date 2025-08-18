"""
МОДУЛЬ: get_exam_by_number.py

ОПИС:
Отримує дослідження з Єнота (Document_Анализы) по номеру дослідження.

Що робить:
- Шукає документ за Number
- Перевіряє, що Posted == true
- Витягує Ref_Key, дату, тип, клінічну причину з Состав
- Отримує пацієнта з Catalog_Карточки
- Обчислює вік на момент дослідження
- Витягує останню вагу з InformationRegister_Взвешивание
- Записує або оновлює дані в таблиці xr_study_requests
"""

import os
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime, date

# === Завантаження .env ===
load_dotenv()

ODATA_URL = os.getenv("ODATA_URL_COPY").rstrip("/")
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

def calculate_age(birthdate: str, exam_date: str) -> float:
    birth = datetime.strptime(birthdate[:10], "%Y-%m-%d").date()
    exam = datetime.strptime(exam_date[:10], "%Y-%m-%d").date()
    delta = (exam - birth).days
    return round(delta / 365.25, 2)


def get_exam_by_number(exam_number: str = None):
    if not exam_number:
        exam_number = "000001534"
        print(f"[DEBUG] Тестовий запуск. Використовується номер: {exam_number}")

    # === 1. Document_Анализы ===
    url = f"{ODATA_URL}/Document_Анализы?$filter=Number eq '{exam_number}'&$format=json"
    response = requests.get(url, auth=(ODATA_USER, ODATA_PASSWORD))
    data = response.json().get("value", [])

    if not data:
        print(f"[❌] Дослідження №{exam_number} не знайдено.")
        return

    doc = data[0]
    if not doc.get("Posted", False):
        print(f"[⏸] Дослідження №{exam_number} не проведене.")
        return

    ref_key_exam = doc["Ref_Key"]
    study_number = doc["Number"]
    exam_date = doc["Date"]
    exam_type = doc["ТипСобытия"]
    patient_key = doc["Карточка_Key"]

    # === Витяг "Комментарий" з Состав ===
    study_reason = ""
    for row in doc.get("Состав", []):
        if (row.get("Вопрос_Key") == "2442d6fa-396f-11f0-9971-2ae983d8a0f0" and
                row.get("ЭлементарныйВопрос_Key") == "1fe85aa8-396f-11f0-9971-2ae983d8a0f0"):
            study_reason = row.get("ОткрытыйОтвет", "")
            break

    # === 2. Catalog_Карточки ===
    patient_url = f"{ODATA_URL}/Catalog_Карточки(guid'{patient_key}')?$format=json"
    pat = requests.get(patient_url, auth=(ODATA_USER, ODATA_PASSWORD)).json()

    patient_name = pat.get("Description", "")
    species_key = pat.get("Вид_Key")
    breed_key = pat.get("Порода_Key")
    sex_key = pat.get("Пол")
    age_years = None

    if pat.get("ДатаРождения"):
        try:
            age_years = calculate_age(pat["ДатаРождения"], exam_date)
        except Exception as e:
            print(f"[WARN] Не вдалося розрахувати вік: {e}")

    # === 3. Вага з InformationRegister_Взвешивание ===
    weight_url = f"{ODATA_URL}/InformationRegister_Взвешивание?$filter=Карточка_Key eq guid'{patient_key}'&$orderby=Period desc&$top=1&$format=json"
    w_resp = requests.get(weight_url, auth=(ODATA_USER, ODATA_PASSWORD))
    weight_data = w_resp.json().get("value", [])

    weight_kg = None
    weight_measured_at = None
    if weight_data:
        weight_kg = weight_data[0].get("Вес")
        weight_measured_at = weight_data[0].get("Period", "")[:10]

    weight_days_old = None
    if weight_measured_at:
        try:
            dt_measured = datetime.strptime(weight_measured_at, "%Y-%m-%d").date()
            dt_exam = datetime.strptime(exam_date[:10], "%Y-%m-%d").date()
            weight_days_old = (dt_exam - dt_measured).days
        except Exception as e:
            print(f"[WARN] Не вдалося обчислити давність зважування: {e}")


    # === 4. Запис у БД ===
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )
    cursor = conn.cursor()

    sql = """
    INSERT INTO xr_study_requests (
        ref_key_exam, study_number, study_reason, exam_date, exam_type,
        patient_name, species_key, breed_key, sex_key, age_years,
        weight_kg, weight_measured_at, weight_days_old, status
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        study_reason = VALUES(study_reason),
        exam_date = VALUES(exam_date),
        exam_type = VALUES(exam_type),
        patient_name = VALUES(patient_name),
        species_key = VALUES(species_key),
        breed_key = VALUES(breed_key),
        sex_key = VALUES(sex_key),
        age_years = VALUES(age_years),
        weight_kg = VALUES(weight_kg),
        weight_measured_at = VALUES(weight_measured_at),
        weight_days_old = VALUES(weight_days_old),
        status = 'waiting',
        updated_at = NOW()
    """

    values = (
        ref_key_exam,
        study_number,
        study_reason,
        exam_date[:10],
        exam_type,
        patient_name,
        species_key,
        breed_key,
        sex_key,
        age_years,
        weight_kg,
        weight_measured_at,
        weight_days_old,
        "waiting"  # ← вставка
    )


    cursor.execute(sql, values)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[✅] Дослідження №{study_number} ({ref_key_exam}) збережено.")


# === Тестовий запуск ===
if __name__ == "__main__":
    get_exam_by_number()
