import json
import requests
import mysql.connector
import time
from dotenv import load_dotenv
import os

# === Завантаження змінних з .env ===
load_dotenv()
ODATA_BASE_URL = os.getenv('ODATA_URL')
ODATA_USER = os.getenv('ODATA_USER')
ODATA_PASSWORD = os.getenv('ODATA_PASSWORD')
DB_HOST = os.getenv('DB_HOST_Serv')
DB_PORT = int(os.getenv('DB_PORT_Serv', 3306))
DB_USER = os.getenv('DB_USER_Serv')
DB_PASSWORD = os.getenv('DB_PASSWORD_Serv')
DB_DATABASE = os.getenv('DB_DATABASE_Serv')

# === Параметри ===
TABLE_NAME = "et_Catalog_Номенклатура"
BATCH_SIZE = 1000
SLEEP_SECONDS = 1

# === Поля, які є в БД ===
FIELDS = [
    'Ref_Key', 'DataVersion', 'DeletionMark', 'Parent_Key', 'IsFolder', 'Code', 'Description',
    'SOVA_UDSМаксимальныйПроцентОплатыБаллами', 'SOVA_UDSНеПрименятьСкидку', 'SOVA_UDSПроцентДополнительногоНачисления', 
    'АналитикаПоЗарплате_Key', 'Артикул', 'Весовой', 'ВестиУчетПоСериям', 'Вид_Key', 'ВидНоменклатуры',
    'ЕдиницаБазовая_Key', 'ЕдиницаДляОтчетов_Key', 'ЕдиницаИнвентаризации_Key', 'ЕдиницаПоставок_Key', 'ЕдиницаРозницы_Key', 
    'ЕдиницаФиксированная_Key', 'ЕдиницаХраненияОстатков_Key', 'ЕдиницаЦены_Key', 'ЗапрещенаРозничнаяТорговля', 'КодВнешнейБазы',
    'Predefined', 'PredefinedDataName'
]

# === Підключення до БД ===
conn = mysql.connector.connect(
    host=DB_HOST, port=DB_PORT,
    user=DB_USER, password=DB_PASSWORD,
    database=DB_DATABASE, charset='utf8mb4'
)
cursor = conn.cursor()

# === Основна логіка ===
page_number = 0
total_records = 0

while True:
    print(f"\n🔄 Обробка сторінки {page_number + 1}...")
    url = f"{ODATA_BASE_URL}Catalog_Номенклатура?$format=json&$orderby=Ref_Key&$top={BATCH_SIZE}&$skip={page_number * BATCH_SIZE}"
    
    t0 = time.time()
    response = requests.get(url, auth=(ODATA_USER, ODATA_PASSWORD))
    t1 = time.time()

    response.raise_for_status()
    entries = response.json().get('value', [])
    if not entries:
        break

    print(f"🕐 Отримано {len(entries)} записів за {round(t1 - t0, 1)} сек")

    values = []
    for entry in entries:
        row = tuple(entry.get(field, None) for field in FIELDS)
        values.append(row)

    placeholders = ', '.join(['%s'] * len(FIELDS))
    columns = ', '.join([f"`{field}`" for field in FIELDS])
    update_clause = ', '.join([f"`{field}`=VALUES(`{field}`)" for field in FIELDS if field != 'Ref_Key'])

    insert_sql = f"""
        INSERT INTO {TABLE_NAME} ({columns})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """

    t2 = time.time()
    cursor.executemany(insert_sql, values)
    conn.commit()
    t3 = time.time()

    print(f"✅ Записано {len(values)} записів у БД за {round(t3 - t2, 1)} сек")
    total_records += len(values)

    page_number += 1
    time.sleep(SLEEP_SECONDS)

# === Завершення ===
cursor.close()
conn.close()
print(f"\n🏁 Успішно перенесено {total_records} записів у {TABLE_NAME}")
