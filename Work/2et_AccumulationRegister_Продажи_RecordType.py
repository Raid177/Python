import os
import time
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

# === КОНФІГУРАЦІЯ ===
BATCH_SIZE = 2000                  # Кількість записів у порції
SLEEP_SECONDS = 0.5                 # Пауза між запитами
DAYS_BACK = 45                    # Наскільки днів назад тягнути з OData
DELETE_OLD_RECORDS = True        # Якщо True — видаляти старі записи перед вставкою

# === Змінні середовища ===
load_dotenv()

DB_HOST = os.getenv("DB_HOST_Serv", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT_Serv", 3306))
DB_USER = os.getenv("DB_USER_Serv")
DB_PASSWORD = os.getenv("DB_PASSWORD_Serv")
DB_DATABASE = os.getenv("DB_DATABASE_Serv")

ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")
ODATA_URL = os.getenv("ODATA_URL")
if not ODATA_URL:
    raise ValueError("ODATA_URL не знайдено в .env")

# === Отримати останній Period у БД ===
def get_last_period():
    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_DATABASE
    )
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(Period) FROM et_AccumulationRegister_Продажи_RecordType")
    last_period = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    if last_period:
        return last_period - timedelta(days=DAYS_BACK)
    return datetime(2024, 7, 20)

# === Очистити старі записи (якщо DELETE_OLD_RECORDS = True) ===
def delete_old_records(from_date):
    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_DATABASE
    )
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM et_AccumulationRegister_Продажи_RecordType WHERE Period >= %s", (from_date,)
    )
    deleted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    print(f"🗑️ Видалено {deleted} записів з Period >= {from_date}")

# === Основна функція ===
def fetch_and_store_data():
    start_date = get_last_period()
    if DELETE_OLD_RECORDS:
        delete_old_records(start_date)

    start_date_filter = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    base_url = (
        f"{ODATA_URL.rstrip('/')}/AccumulationRegister_Продажи_RecordType?$format=json"
        f"&$filter=Period ge datetime'{start_date_filter}'"
        f"&$orderby=Period"
    )

    session = requests.Session()
    session.auth = (ODATA_USER, ODATA_PASSWORD)

    insert_query = """
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
    """

    skip = 0
    while True:
        url = f"{base_url}&$top={BATCH_SIZE}&$skip={skip}"
        response = session.get(url)

        if response.status_code != 200:
            print(f"❌ Помилка запиту: {response.status_code} {response.text}")
            break

        data = response.json().get("value", [])
        if not data:
            print("✅ Отримано всі записи.")
            break

        min_period = min(d["Period"] for d in data)
        max_period = max(d["Period"] for d in data)
        print(f"📦 Отримано {len(data)} записів ({skip + 1} - {skip + len(data)}) з {min_period} по {max_period}")

        conn = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, database=DB_DATABASE
        )
        cursor = conn.cursor()
        try:
            start = time.time()
            cursor.executemany(insert_query, data)
            conn.commit()
            print(f"✅ Успішно вставлено {len(data)} записів за {time.time() - start:.2f} сек.")
        except Exception as e:
            print(f"❌ Помилка вставки: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

        skip += len(data)
        time.sleep(SLEEP_SECONDS)

# === Старт ===
fetch_and_store_data()
