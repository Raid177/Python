import json
import requests
import mysql.connector
from dotenv import load_dotenv
import os

# Завантаження змінних з .env
load_dotenv()

# Параметри для підключення до 1С та БД з .env
ODATA_URL = os.getenv('ODATA_URL') + 'Catalog_Контрагенты?$format=json'
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

added_count = 0
updated_count = 0

# Вставка або оновлення даних у БД
for entry in entries:
    sklad = {q['Вопрос_Key']: q['Ответ'] for q in entry.get('Состав', [])}
    edrpou = sklad.get('c53d792c-4ef4-11ef-87da-2ae983d8a0f0')
    iban1 = sklad.get('f61f85c6-4ef4-11ef-87da-2ae983d8a0f0')
    iban2 = sklad.get('42667dea-4ef5-11ef-87da-2ae983d8a0f0')

    cursor.execute("SELECT DataVersion FROM et_counterparties WHERE Ref_Key = %s", (entry['Ref_Key'],))
    result = cursor.fetchone()

    if not result or result[0] != entry['DataVersion']:
        cursor.execute(""" 
        INSERT INTO et_counterparties 
        (Ref_Key, DataVersion, DeletionMark, Parent_Key, IsFolder, Code, Description,
         ТипЦен_Key, ВалютаВзаиморасчетов_Key, КонтактнаяИнформация, Комментарий,
         ОтсрочкаПлатежа, КодВнешнейБазы, Менеджер_Key, ПремияПолучена, АнкетаЗаполнена,
         ЭтоВнешняяЛаборатория, ЭтоПоставщик, ЭтоРеферент, ИНН, ЕДРПОУ, IBAN1, IBAN2)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        DataVersion = VALUES(DataVersion),
        Description = VALUES(Description),
        ЕДРПОУ = VALUES(ЕДРПОУ),
        IBAN1 = VALUES(IBAN1),
        IBAN2 = VALUES(IBAN2)
        """, (
            entry['Ref_Key'], entry['DataVersion'], entry['DeletionMark'], entry['Parent_Key'],
            entry['IsFolder'], entry['Code'], entry['Description'], entry['ТипЦен_Key'],
            entry['ВалютаВзаиморасчетов_Key'], entry['КонтактнаяИнформация'], entry['Комментарий'],
            entry['ОтсрочкаПлатежа'], entry['КодВнешнейБазы'], entry['Менеджер_Key'],
            entry['ПремияПолучена'], entry['АнкетаЗаполнена'], entry['ЭтоВнешняяЛаборатория'],
            entry['ЭтоПоставщик'], entry['ЭтоРеферент'], entry['ИНН'], edrpou, iban1, iban2
        ))

        if cursor.rowcount > 0:
            if not result:
                added_count += 1
            else:
                updated_count += 1

# Завершення роботи
conn.commit()
cursor.close()
conn.close()

# Вивід статистики
print(f"✅ Перенесення повних даних із вкладеними полями виконано успішно.")
print(f"📌 Додано нових записів: {added_count}")
print(f"🔄 Оновлено записів: {updated_count}")
