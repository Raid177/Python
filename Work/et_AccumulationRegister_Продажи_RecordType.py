import os
import time
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Завантаження змінних середовища
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")
ODATA_URL = os.getenv("ODATA_URL")

if not ODATA_URL:
    raise ValueError("ODATA_URL не знайдено в змінних середовища")

# Функція отримання останньої дати Period з БД
def get_last_period():
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(Period) FROM et_AccumulationRegister_Продажи_RecordType")
    last_period = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    if last_period:
        return last_period - timedelta(days=45)
    return datetime(2024, 7, 20)  # Якщо немає записів, беремо 450 днів тому

# Визначаємо дату для фільтрації
start_date_filter = get_last_period().strftime("%Y-%m-%dT%H:%M:%S")

# Базовий URL з фільтром і сортуванням
BASE_ODATA_URL = (
    f"{ODATA_URL}AccumulationRegister_Продажи_RecordType?$format=json"
    f"&$filter=Period ge datetime'{start_date_filter}'"
    f"&$orderby=Period"
)

# Підключення до MySQL
def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

# Функція отримання та запису даних
def fetch_and_store_data():
    session = requests.Session()
    session.auth = (ODATA_USER, ODATA_PASSWORD)
    
    skip = 0  # Лічильник пропущених записів
    
    while True:
        next_url = f"{BASE_ODATA_URL}&$top=1000&$skip={skip}"
        response = session.get(next_url)

        if response.status_code != 200:
            print(f"Помилка запиту: {response.status_code} {response.text}")
            break

        data = response.json().get("value", [])
        if not data:
            print("Дані відсутні або отримано всі записи.")
            break

        min_period = min(d["Period"] for d in data)
        max_period = max(d["Period"] for d in data)
        print(f"Отримано {len(data)} записів ({skip + 1} - {skip + len(data)}) з {min_period} по {max_period}")

        conn = get_db_connection()
        cursor = conn.cursor()

        for record in data:
            cursor.execute(
                """
                INSERT INTO et_AccumulationRegister_Продажи_RecordType (
                    Recorder, LineNumber, Period, Recorder_Type, Active, Номенклатура_Key, Организация_Key, Подразделение_Key, 
                    Контрагент_Key, Карточка_Key, Сотрудник, Сотрудник_Type, Исполнитель, Исполнитель_Type, Склад_Key, 
                    ОрганизацияИсполнителя_Key, ПодразделениеИсполнителя_Key, Количество, КоличествоОплачено, Стоимость, 
                    СтоимостьБезСкидок, СуммаЗатрат, created_at, updated_at
                ) VALUES (
                    %(Recorder)s, %(LineNumber)s, %(Period)s, %(Recorder_Type)s, %(Active)s, %(Номенклатура_Key)s, %(Организация_Key)s, %(Подразделение_Key)s, 
                    %(Контрагент_Key)s, %(Карточка_Key)s, %(Сотрудник)s, %(Сотрудник_Type)s, %(Исполнитель)s, %(Исполнитель_Type)s, %(Склад_Key)s, 
                    %(ОрганизацияИсполнителя_Key)s, %(ПодразделениеИсполнителя_Key)s, %(Количество)s, %(КоличествоОплачено)s, %(Стоимость)s, 
                    %(СтоимостьБезСкидок)s, %(СуммаЗатрат)s, NOW(), NOW()
                )
                ON DUPLICATE KEY UPDATE 
                    Period = VALUES(Period), Recorder_Type = VALUES(Recorder_Type), Active = VALUES(Active), Номенклатура_Key = VALUES(Номенклатура_Key),
                    Организация_Key = VALUES(Организация_Key), Подразделение_Key = VALUES(Подразделение_Key), Контрагент_Key = VALUES(Контрагент_Key),
                    Карточка_Key = VALUES(Карточка_Key), Сотрудник = VALUES(Сотрудник), Сотрудник_Type = VALUES(Сотрудник_Type), Исполнитель = VALUES(Исполнитель),
                    Исполнитель_Type = VALUES(Исполнитель_Type), Склад_Key = VALUES(Склад_Key), ОрганизацияИсполнителя_Key = VALUES(ОрганизацияИсполнителя_Key),
                    ПодразделениеИсполнителя_Key = VALUES(ПодразделениеИсполнителя_Key), Количество = VALUES(Количество), КоличествоОплачено = VALUES(КоличествоОплачено),
                    Стоимость = VALUES(Стоимость), СтоимостьБезСкидок = VALUES(СтоимостьБезСкидок), СуммаЗатрат = VALUES(СуммаЗатрат), updated_at = NOW()
                """,
                record
            )

        conn.commit()
        cursor.close()
        conn.close()

        # Чекаємо перед наступним запитом
        time.sleep(1)

        # Збільшуємо лічильник skip для отримання наступної порції
        skip += len(data)

fetch_and_store_data()
