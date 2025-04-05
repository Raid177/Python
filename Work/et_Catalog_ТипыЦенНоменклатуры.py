import os
import requests
import mysql.connector
from dotenv import load_dotenv

# Завантаження змінних з .env
load_dotenv()

ODATA_URL = os.getenv("ODATA_URL") + "Catalog_ТипыЦенНоменклатуры"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_DATABASE"),
    'charset': 'utf8mb4'
}

# Отримання даних з OData
response = requests.get(
    os.getenv("ODATA_URL") + "Catalog_ТипыЦенНоменклатуры?$format=json",
    auth=(ODATA_USER, ODATA_PASSWORD)
)
data = response.json().get("value", [])

# Підключення до БД
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

insert_query = """
INSERT INTO et_Catalog_ТипыЦенНоменклатуры (
    Ref_Key, DataVersion, DeletionMark, Code, Description,
    ВалютаЦены_Key, БазовыйТипЦен_Key, Рассчитывается,
    ПроцентСкидкиНаценки, ПорядокОкругления, ОкруглятьВБольшуюСторону,
    Комментарий, СпособРасчетаЦены, Срочность, ID,
    Predefined, PredefinedDataName, created_at, updated_at
) VALUES (
    %(Ref_Key)s, %(DataVersion)s, %(DeletionMark)s, %(Code)s, %(Description)s,
    %(ВалютаЦены_Key)s, %(БазовыйТипЦен_Key)s, %(Рассчитывается)s,
    %(ПроцентСкидкиНаценки)s, %(ПорядокОкругления)s, %(ОкруглятьВБольшуюСторону)s,
    %(Комментарий)s, %(СпособРасчетаЦены)s, %(Срочность)s, %(ID)s,
    %(Predefined)s, %(PredefinedDataName)s, NOW(), NOW()
) ON DUPLICATE KEY UPDATE
    DataVersion = VALUES(DataVersion),
    DeletionMark = VALUES(DeletionMark),
    Code = VALUES(Code),
    Description = VALUES(Description),
    ВалютаЦены_Key = VALUES(ВалютаЦены_Key),
    БазовыйТипЦен_Key = VALUES(БазовыйТипЦен_Key),
    Рассчитывается = VALUES(Рассчитывается),
    ПроцентСкидкиНаценки = VALUES(ПроцентСкидкиНаценки),
    ПорядокОкругления = VALUES(ПорядокОкругления),
    ОкруглятьВБольшуюСторону = VALUES(ОкруглятьВБольшуюСторону),
    Комментарий = VALUES(Комментарий),
    СпособРасчетаЦены = VALUES(СпособРасчетаЦены),
    Срочность = VALUES(Срочность),
    ID = VALUES(ID),
    Predefined = VALUES(Predefined),
    PredefinedDataName = VALUES(PredefinedDataName),
    updated_at = NOW()
"""

count_inserted = 0
count_updated = 0

for item in data:
    cursor.execute("SELECT DataVersion FROM et_Catalog_ТипыЦенНоменклатуры WHERE Ref_Key = %s", (item["Ref_Key"],))
    result = cursor.fetchone()
    
    if result is None:
        count_inserted += 1
    elif result[0] != item["DataVersion"]:
        count_updated += 1
    else:
        continue  # DataVersion збігається — нічого не робимо

    cursor.execute(insert_query, item)

conn.commit()
cursor.close()
conn.close()

print(f"Імпорт завершено: нових записів — {count_inserted}, оновлених — {count_updated}")
