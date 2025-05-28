# Ruslana Bondar-Sydorenko, [25.05.2025 22:22]
# Да. Если заполнена сами результатов в документе, но галка не установлена Результаты получены - то движений по данному регистру нет.

# Ruslana Bondar-Sydorenko, [25.05.2025 22:23]
# т.е не сохраняются ответы в регистр. В документе они конечно же хранятся.


import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import dotenv_values
import pymysql

# ==== Завантаження конфігурації з .env ====
env = dotenv_values("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

ODATA_URL = env["ODATA_URL"]
ODATA_USER = env["ODATA_USER"]
ODATA_PASSWORD = env["ODATA_PASSWORD"]
conn = pymysql.connect(
    host=env["DB_HOST"],
    user=env["DB_USER"],
    password=env["DB_PASSWORD"],
    database=env["DB_DATABASE"],
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# ==== Вхідні дані (вручну або з іншого джерела) ====
study_number = "000001957"
valid_rentgen_type_refs = [
    "4f777428-07e8-11e5-80ce-00155dd6780b",  # Рентген
    # "додати ще типи за потреби"
]
question_key = "1fe85aa8-396f-11f0-9971-2ae983d8a0f0"

# Нормалізація номера дослідження до 9 символів з ведучими нулями
study_number = study_number.zfill(9)

# ==== 1. Отримання дослідження ====
doc_url = f"{ODATA_URL}Document_Анализы?$format=json&$filter=Number eq '{study_number}'"
doc_response = requests.get(doc_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD))
doc_data = doc_response.json()

if not doc_data.get("value"):
    exit("❌ Дослідження з таким номером не знайдено.")

doc = doc_data["value"][0]
doc_ref = doc["Ref_Key"]
doc_ref = doc["Ref_Key"]
study_date = doc.get("Date", "")[:10]  # Для віку на момент дослідження

# ==== Перевірка відповідності ====
posted = doc.get("Posted", False)
summa = doc.get("СуммаДокумента", 0)
event_ref = doc.get("ТипСобытия", None)

if not posted or summa <= 1 or event_ref not in valid_rentgen_type_refs:
    exit("⚠️ Це не рентген-дослідження або воно неоплачене / не проведене.")

# ==== 2. Витяг ОткрытогоОтвета з вкладеного Состава ====
open_answer = "-"
sostav = doc.get("Состав", [])
for row in sostav:
    if row.get("ЭлементарныйВопрос_Key") == question_key:
        open_answer = row.get("ОткрытыйОтвет", "").strip()
        break

# ==== 3. Отримання даних пацієнта ====
patient_key = doc["Карточка_Key"]
patient_url = f"{ODATA_URL}Catalog_Карточки(guid'{patient_key}')?$format=json"
patient = requests.get(patient_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD)).json()

name = patient.get("Description", "")
contract = patient.get("НомерДоговора", "")
owner = patient.get("Code", "")
birthdate = patient.get("ДатаРождения", "")
gender = patient.get("Пол", "")
species_key = patient.get("Вид_Key", None)
breed_key = patient.get("Порода_Key", None)

# ==== 4. Отримання виду ====
species = ""
if species_key:
    species_url = f"{ODATA_URL}Catalog_Породы(guid'{species_key}')?$format=json"
    species = requests.get(species_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD)).json().get("Description", "")

# ==== 5. Отримання породи ====
breed = ""
if breed_key:
    breed_url = f"{ODATA_URL}Catalog_Породы(guid'{breed_key}')?$format=json"
    breed = requests.get(breed_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD)).json().get("Description", "")

# ==== 6. Вік тварини (на момент дослідження) ====
age = ""
if birthdate and study_date:
    try:
        birthdate_dt = datetime.strptime(birthdate[:10], "%Y-%m-%d")
        study_date_dt = datetime.strptime(study_date, "%Y-%m-%d")
        diff = relativedelta(study_date_dt, birthdate_dt)
        age = f"{diff.years}р {diff.months}м"
    except:
        age = "невідомо"

# ==== 7. Останнє зважування ====
weight_url = (
    f"{ODATA_URL}InformationRegister_Взвешивание?"
    f"$format=json&$filter=Карточка_Key eq guid'{patient_key}'&$orderby=Period desc&$top=1"
)
weight_response = requests.get(weight_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD))
weight_data = weight_response.json().get("value", [])

weight = "-"
weight_date = "-"
if weight_data:
    weight = weight_data[0].get("Вес", "-")
    weight_date = weight_data[0].get("Period", "")[:10]

# ==== 8. Вивід результату ====
print("\n" + "=" * 60)
print(f"🔎 Інформація по дослідженню № {study_number}")
print("=" * 60)
print(f"🆔 Ref_Key документа:  {doc_ref}")
print(f"📌 Кличка:            {name}")
print(f"📄 Номер договору:    {contract}")
print(f"👤 Власник (ПІБ):     {owner}")
print(f"🧬 Вид:               {species}")
print(f"🐾 Порода:            {breed}")
print(f"⚥ Стать:              {gender}")
print(f"🎂 Вік:               {age} (на дату {study_date})")
print(f"⚖️  Вага:              {weight} кг (від {weight_date})")
print(f"📝 Показання:          {open_answer}")
print("=" * 60)

with conn.cursor() as cursor:
    sql = """
        INSERT INTO bot_study_requests (
            path_image, type_exam, id_patient, Ref_KeyEXAM, date_exam,
            name, owner, kind, breed, sex, age, weight,
            exam_context, requested_by, status, created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, NOW(), NOW()
        )
        ON DUPLICATE KEY UPDATE
            path_image = VALUES(path_image),
            type_exam = VALUES(type_exam),
            id_patient = VALUES(id_patient),
            date_exam = VALUES(date_exam),
            name = VALUES(name),
            owner = VALUES(owner),
            kind = VALUES(kind),
            breed = VALUES(breed),
            sex = VALUES(sex),
            age = VALUES(age),
            weight = VALUES(weight),
            exam_context = VALUES(exam_context),
            requested_by = VALUES(requested_by),
            status = VALUES(status),
            updated_at = NOW();
    """

    cursor.execute(sql, (
        f'Study_Exam/{doc_ref}',  # path_image
        'Rh_torax',              # type_exam
        contract,                # id_patient
        doc_ref,                 # Ref_KeyEXAM
        study_date,              # date_exam

        name,                    # name
        owner,                   # owner
        species,                 # kind
        breed,                   # breed
        gender,                  # sex
        age,                     # age
        weight,                  # weight

        open_answer,            # exam_context
        0,                      # requested_by (на цьому етапі не через ТГ)
        'pending'               # status
    ))

    conn.commit()
conn.close()
