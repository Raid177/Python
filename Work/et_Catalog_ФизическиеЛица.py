import json
import requests
import mysql.connector
from dotenv import load_dotenv
import os

# Завантажуємо змінні з .env файлу
load_dotenv()

# Параметри для підключення до 1С та БД з .env
ODATA_URL = os.getenv('ODATA_URL') + 'Catalog_ФизическиеЛица?$format=json'
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
    cursor.execute("SELECT DataVersion FROM et_Catalog_ФизическиеЛица WHERE Ref_Key = %s", (entry['Ref_Key'],))
    result = cursor.fetchone()

    if not result or result[0] != entry['DataVersion']:
        cursor.execute("""
            INSERT INTO et_Catalog_ФизическиеЛица 
            (Ref_Key, DataVersion, DeletionMark, Parent_Key, IsFolder, Code, Description, ДатаРождения, Комментарий, 
             Должность_Key, ИспользоватьГрафикРаботы, УДАЛИТЬРазрешитьВыбиратьБезПароля, УДАЛИТЬНаборПрав_Key, 
             УДАЛИТЬВариантПечати, НеПоказыватьВГрафикеРаботы, SOVA_UDSВнешнийИдентификатор, ДатаПриема, ДатаУвольнения, 
             Роль_Key, ДопРоль1_Key, ДопРоль2_Key, ДопРоль3_Key, МПК_ЗаписьРазрешена, МПК_ПредставлениеДолжности, 
             МПК_ФотоСотрудника_Key, ID, ИНН, Predefined, PredefinedDataName)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            DataVersion = VALUES(DataVersion),
            Description = VALUES(Description),
            ДатаРождения = VALUES(ДатаРождения),
            Комментарий = VALUES(Комментарий),
            Должность_Key = VALUES(Должность_Key),
            ДатаПриема = VALUES(ДатаПриема),
            ДатаУвольнения = VALUES(ДатаУвольнения)
        """, (
            entry['Ref_Key'], entry['DataVersion'], entry['DeletionMark'], entry['Parent_Key'], entry['IsFolder'],
            entry['Code'], entry['Description'], entry.get('ДатаРождения'), entry.get('Комментарий'),
            entry.get('Должность_Key'), entry.get('ИспользоватьГрафикРаботы'), entry.get('УДАЛИТЬРазрешитьВыбиратьБезПароля'),
            entry.get('УДАЛИТЬНаборПрав_Key'), entry.get('УДАЛИТЬВариантПечати'), entry.get('НеПоказыватьВГрафикеРаботы'),
            entry.get('SOVA_UDSВнешнийИдентификатор'), entry.get('ДатаПриема'), entry.get('ДатаУвольнения'),
            entry.get('Роль_Key'), entry.get('ДопРоль1_Key'), entry.get('ДопРоль2_Key'), entry.get('ДопРоль3_Key'),
            entry.get('МПК_ЗаписьРазрешена'), entry.get('МПК_ПредставлениеДолжности'), entry.get('МПК_ФотоСотрудника_Key'),
            entry.get('ID', ''), entry.get('ИНН'), entry['Predefined'], entry['PredefinedDataName']
        ))

# Завершення роботи
conn.commit()
cursor.close()
conn.close()
print("✅ Дані успішно перенесені в et_Catalog_ФизическиеЛица")