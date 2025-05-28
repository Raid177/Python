import os
import datetime
import asyncio
import pymysql
import requests
from dotenv import dotenv_values
from requests.auth import HTTPBasicAuth
from dateutil.relativedelta import relativedelta

def get_patient_data(study_number):
    env = dotenv_values("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
    ODATA_URL = env["ODATA_URL_COPY"]
    ODATA_USER = env["ODATA_USER"]
    ODATA_PASSWORD = env["ODATA_PASSWORD"]

    study_number = study_number.zfill(9)
    doc_url = f"{ODATA_URL}Document_Анализы?$format=json&$filter=Number eq '{study_number}'"
    doc_response = requests.get(doc_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD))
    doc_data = doc_response.json()

    if not doc_data.get("value"):
        return {"success": False, "error": "Дослідження з таким номером не знайдено."}

    doc = doc_data["value"][0]
    doc_ref = doc["Ref_Key"]
    study_date = doc.get("Date", "")[:10]

    posted = doc.get("Posted", False)
    summa = doc.get("СуммаДокумента", 0)
    event_ref = doc.get("ТипСобытия", None)
    valid_rentgen_type_refs = ["4f777428-07e8-11e5-80ce-00155dd6780b"]

    if not posted or summa <= 1 or event_ref not in valid_rentgen_type_refs:
        return {"success": False, "error": "Це не рентген-дослідження або воно неоплачене / не проведене."}

    open_answer = "-"
    question_key = "1fe85aa8-396f-11f0-9971-2ae983d8a0f0"
    for row in doc.get("Состав", []):
        if row.get("ЭлементарныйВопрос_Key") == question_key:
            open_answer = row.get("ОткрытыйОтвет", "").strip()
            break

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

    species = ""
    if species_key:
        species_url = f"{ODATA_URL}Catalog_Породы(guid'{species_key}')?$format=json"
        species = requests.get(species_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD)).json().get("Description", "")

    breed = ""
    if breed_key:
        breed_url = f"{ODATA_URL}Catalog_Породы(guid'{breed_key}')?$format=json"
        breed = requests.get(breed_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD)).json().get("Description", "")

    age = ""
    if birthdate and study_date:
        try:
            birthdate_dt = datetime.datetime.strptime(birthdate[:10], "%Y-%m-%d")
            study_date_dt = datetime.datetime.strptime(study_date, "%Y-%m-%d")
            diff = relativedelta(study_date_dt, birthdate_dt)
            age = f"{diff.years}р {diff.months}м"
        except:
            age = "невідомо"

    weight_url = (
        f"{ODATA_URL}InformationRegister_Взвешивание?"
        f"$format=json&$filter=Карточка_Key eq guid'{patient_key}'&$orderby=Period desc&$top=1"
    )
    weight_response = requests.get(weight_url, auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD))
    weight_data = weight_response.json().get("value", [])

    weight = "-"
    if weight_data:
        weight = weight_data[0].get("Вес", "-")

    return {
        "success": True,
        "Ref_KeyEXAM": doc_ref,
        "date_exam": study_date,
        "name": name,
        "id_patient": contract,
        "owner": owner,
        "kind": species,
        "breed": breed,
        "sex": gender,
        "age": age,
        "weight": weight,
        "exam_context": open_answer,
    }

def insert_study_request(patient_info, save_dir, requested_by, n_files):
    env = dotenv_values("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
    conn = pymysql.connect(
        host=env["DB_HOST"],
        user=env["DB_USER"],
        password=env["DB_PASSWORD"],
        database=env["DB_DATABASE"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    with conn.cursor() as cursor:
        sql = """
            INSERT INTO bot_study_requests (
                path_image, type_exam, id_patient, Ref_KeyEXAM, date_exam,
                name, owner, kind, breed, sex, age, weight,
                exam_context, requested_by, status, created_at, updated_at, n_files
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, NOW(), NOW(), %s
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
                updated_at = NOW(),
                n_files = VALUES(n_files);
        """

        cursor.execute(sql, (
            save_dir.replace("\\", "/"),  # Windows-safe path
            'Rh_torax',
            patient_info['id_patient'],
            patient_info['Ref_KeyEXAM'],
            patient_info['date_exam'],
            patient_info['name'],
            patient_info['owner'],
            patient_info['kind'],
            patient_info['breed'],
            patient_info['sex'],
            patient_info['age'],
            patient_info['weight'],
            patient_info['exam_context'],
            requested_by,
            'pending',
            n_files
        ))

        conn.commit()
    conn.close()
