# Ruslana Bondar-Sydorenko, [25.05.2025 22:22]
# Да. Если заполнена сами результатов в документе, но галка не установлена Результаты получены - то движений по данному регистру нет.

# Ruslana Bondar-Sydorenko, [25.05.2025 22:23]
# т.е не сохраняются ответы в регистр. В документе они конечно же хранятся.


import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import dotenv_values

# ==== Завантаження конфігурації з .env ====
env = dotenv_values("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

ODATA_URL = env["ODATA_URL"]
ODATA_USER = env["ODATA_USER"]
ODATA_PASSWORD = env["ODATA_PASSWORD"]

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

# ==== 6. Вік тварини ====
age = ""
if birthdate:
    try:
        birthdate_dt = datetime.strptime(birthdate[:10], "%Y-%m-%d")
        now = datetime.now()
        diff = relativedelta(now, birthdate_dt)
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
print(f"📌 Кличка:            {name}")
print(f"📄 Номер договору:    {contract}")
print(f"👤 Власник (ПІБ):     {owner}")
print(f"🧬 Вид:               {species}")
print(f"🐾 Порода:            {breed}")
print(f"⚥ Стать:             {gender}")
print(f"🎂 Вік:               {age}")
print(f"⚖️  Вага:              {weight} кг (від {weight_date})")
print(f"📝 Показання:          {open_answer}")
print("=" * 60)
