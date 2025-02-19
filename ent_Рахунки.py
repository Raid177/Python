import json
import requests
import mysql.connector
from dotenv import load_dotenv
import os

# Завантажуємо змінні з .env файлу
load_dotenv()

# Параметри для підключення до 1С та БД з .env
ODATA_URL = os.getenv('ODATA_URL') + 'Catalog_ДенежныеСчета?$format=json'
ODATA_USER = os.getenv('ODATA_USER')
ODATA_PASSWORD = os.getenv('ODATA_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

# Підключення до БД
conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
cursor = conn.cursor()

# Отримання даних з 1С
response = requests.get(ODATA_URL, auth=(ODATA_USER, ODATA_PASSWORD))
response.raise_for_status()
entries = response.json().get('value', [])

# Вставка або оновлення даних у БД
for entry in entries:
    cursor.execute("SELECT DataVersion FROM et_Catalog_ДенежныеСчета WHERE Ref_Key = %s", (entry['Ref_Key'],))
    result = cursor.fetchone()

    if not result or result[0] != entry['DataVersion']:
        cursor.execute("""
            INSERT INTO et_Catalog_ДенежныеСчета 
            (Ref_Key, DataVersion, DeletionMark, Owner_Key, Parent_Key, IsFolder, Code, Description, 
             ВидСчета, НомерСчета, Банк_Key, ПроцентТорговойУступки, ПлавающийПроцентТорговойУступки, 
             КодВнешнейБазы, Predefined, PredefinedDataName, Owner_navigationLinkUrl)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            DataVersion = VALUES(DataVersion),
            Description = VALUES(Description),
            ВидСчета = VALUES(ВидСчета),
            НомерСчета = VALUES(НомерСчета),
            ПроцентТорговойУступки = VALUES(ПроцентТорговойУступки)
        """, (
            entry['Ref_Key'], entry['DataVersion'], entry['DeletionMark'], entry['Owner_Key'],
            entry['Parent_Key'], entry['IsFolder'], entry['Code'], entry['Description'],
            entry['ВидСчета'], entry['НомерСчета'], entry['Банк_Key'], entry['ПроцентТорговойУступки'],
            entry['ПлавающийПроцентТорговойУступки'], entry['КодВнешнейБазы'], entry['Predefined'],
            entry['PredefinedDataName'], entry['Owner@navigationLinkUrl']
        ))

# Завершення роботи
conn.commit()
cursor.close()
conn.close()
print("✅ Дані успішно перенесені в et_Catalog_ДенежныеСчета")
