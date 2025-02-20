import requests
import mysql.connector
import time
import os
from dotenv import load_dotenv

# Завантаження змінних середовища
load_dotenv()

# Параметри підключення до MySQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

# Параметри підключення до OData
ODATA_URL = os.getenv("ODATA_URL") + "AccumulationRegister_Продажи_RecordType?$format=json"
ODATA_AUTH = (os.getenv("ODATA_USER"), os.getenv("ODATA_PASSWORD"))

# SQL-запит для вставки або оновлення записів
UPSERT_QUERY = """
INSERT INTO et_AccumulationRegister_Продажи_RecordType (
    Recorder, LineNumber, Period, Recorder_Type, Active, Номенклатура_Key,
    Организация_Key, Подразделение_Key, Контрагент_Key, Карточка_Key,
    Сотрудник, Сотрудник_Type, Исполнитель, Исполнитель_Type, Склад_Key,
    ОрганизацияИсполнителя_Key, ПодразделениеИсполнителя_Key, Количество,
    КоличествоОплачено, Стоимость, СтоимостьБезСкидок, СуммаЗатрат
) VALUES (
    %(Recorder)s, %(LineNumber)s, %(Period)s, %(Recorder_Type)s, %(Active)s, %(Номенклатура_Key)s,
    %(Организация_Key)s, %(Подразделение_Key)s, %(Контрагент_Key)s, %(Карточка_Key)s,
    %(Сотрудник)s, %(Сотрудник_Type)s, %(Исполнитель)s, %(Исполнитель_Type)s, %(Склад_Key)s,
    %(ОрганизацияИсполнителя_Key)s, %(ПодразделениеИсполнителя_Key)s, %(Количество)s,
    %(КоличествоОплачено)s, %(Стоимость)s, %(СтоимостьБезСкидок)s, %(СуммаЗатрат)s
) ON DUPLICATE KEY UPDATE 
    Period = VALUES(Period),
    Recorder_Type = VALUES(Recorder_Type),
    Active = VALUES(Active),
    Номенклатура_Key = VALUES(Номенклатура_Key),
    Организация_Key = VALUES(Организация_Key),
    Подразделение_Key = VALUES(Подразделение_Key),
    Контрагент_Key = VALUES(Контрагент_Key),
    Карточка_Key = VALUES(Карточка_Key),
    Сотрудник = VALUES(Сотрудник),
    Сотрудник_Type = VALUES(Сотрудник_Type),
    Исполнитель = VALUES(Исполнитель),
    Исполнитель_Type = VALUES(Исполнитель_Type),
    Склад_Key = VALUES(Склад_Key),
    ОрганизацияИсполнителя_Key = VALUES(ОрганизацияИсполнителя_Key),
    ПодразделениеИсполнителя_Key = VALUES(ПодразделениеИсполнителя_Key),
    Количество = VALUES(Количество),
    КоличествоОплачено = VALUES(КоличествоОплачено),
    Стоимость = VALUES(Стоимость),
    СтоимостьБезСкидок = VALUES(СтоимостьБезСкидок),
    СуммаЗатрат = VALUES(СуммаЗатрат);
"""

def fetch_and_store_data():
    """Функція для отримання даних з OData та збереження їх у MySQL."""
    offset = 0
    batch_size = 1000
    while True:
        params = {
            "$orderby": "Period",
            "$top": batch_size,
            "$skip": offset
        }
        response = requests.get(ODATA_URL, auth=ODATA_AUTH, params=params)
        
        if response.status_code != 200:
            print("Помилка отримання даних з OData:", response.text)
            break
        
        data = response.json().get("value", [])
        if not data:
            break  # Вихід, якщо більше немає записів
        
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for record in data:
            cursor.execute(UPSERT_QUERY, record)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        offset += batch_size
        time.sleep(1)  # Пауза між запитами

if __name__ == "__main__":
    fetch_and_store_data()
