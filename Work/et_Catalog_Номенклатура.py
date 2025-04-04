import json
import requests
import mysql.connector
import time
from dotenv import load_dotenv
import os

# Завантажуємо змінні з .env файлу
load_dotenv()

# Параметри для підключення до 1С та БД
ODATA_BASE_URL = os.getenv('ODATA_URL')
ODATA_USER = os.getenv('ODATA_USER')
ODATA_PASSWORD = os.getenv('ODATA_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

# Підключення до БД
conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
cursor = conn.cursor()

batch_size = 1000  # Кількість записів у пачці
page_number = 0  # Лічильник пагінації
total_records = 0  # Загальна кількість отриманих записів

# Перелік полів, які є в БД
db_fields = [
    'Ref_Key', 'DataVersion', 'DeletionMark', 'Parent_Key', 'IsFolder', 'Code', 'Description',
    'SOVA_UDSМаксимальныйПроцентОплатыБаллами', 'SOVA_UDSНеПрименятьСкидку', 'SOVA_UDSПроцентДополнительногоНачисления', 
    'АналитикаПоЗарплате_Key', 'Артикул', 'Весовой', 'ВестиУчетПоСериям', 'Вид_Key', 'ВидНоменклатуры',
    'ЕдиницаБазовая_Key', 'ЕдиницаДляОтчетов_Key', 'ЕдиницаИнвентаризации_Key', 'ЕдиницаПоставок_Key', 'ЕдиницаРозницы_Key', 
    'ЕдиницаФиксированная_Key', 'ЕдиницаХраненияОстатков_Key', 'ЕдиницаЦены_Key', 'ЗапрещенаРозничнаяТорговля', 'КодВнешнейБазы',
    'Predefined', 'PredefinedDataName'
]

while True:
    # Формуємо URL для запиту
    ODATA_URL = f"{ODATA_BASE_URL}Catalog_Номенклатура?$format=json&$orderby=Ref_Key&$top={batch_size}&$skip={page_number * batch_size}"
    
    response = requests.get(ODATA_URL, auth=(ODATA_USER, ODATA_PASSWORD))
    response.raise_for_status()
    entries = response.json().get('value', [])
    
    if not entries:
        break  # Вийти, якщо більше немає записів
    
    total_records += len(entries)
    print(f"Пагінація: {page_number + 1}, Отримано записів всього: {total_records}")
    
    for entry in entries:
        cursor.execute("SELECT DataVersion FROM et_Catalog_Номенклатура WHERE Ref_Key = %s", (entry['Ref_Key'],))
        result = cursor.fetchone()

        if not result or result[0] != entry['DataVersion']:
            values = tuple(entry.get(field) for field in db_fields)
            placeholders = ', '.join(['%s'] * len(db_fields))
            columns = ', '.join(db_fields)
            update_clause = ', '.join([f"{field} = VALUES({field})" for field in db_fields])
            
            cursor.execute(f"""
                INSERT INTO et_Catalog_Номенклатура ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """, values)
    
    conn.commit()
    page_number += 1  # Збільшуємо номер сторінки
    time.sleep(1)  # Пауза між запитами

# Завершення роботи
cursor.close()
conn.close()
print("✅ Дані успішно перенесені в et_Catalog_Номенклатура")
