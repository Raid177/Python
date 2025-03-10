import json
import requests
import mysql.connector
import time
from mysql.connector import Error

# Параметри API
base_url = "https://enote.zoolux.clinic/09e599a6-2486-11e3-0983-08606e6953d2/odata/standard.odata/Catalog_ФизическиеЛица"
api_auth = ('zooluxcab', 'mTuee0m5')  # Логін і пароль для API

# Параметри MySQL
mysql_host = "wealth0.mysql.tools"
mysql_user = "wealth0_raid"
mysql_password = "n5yM5ZT87z"
mysql_db = "wealth0_analytics"

# Підключення до БД
def connect_to_db():
    return mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db
    )

# Отримання даних з API
def fetch_data_from_api():
    response = requests.get(f"{base_url}?$format=json", auth=api_auth)
    response.raise_for_status()
    return response.json().get('value', [])

# Вставка або оновлення даних у БД
def upsert_records_into_mysql(records):
    try:
        connection = connect_to_db()
        if connection.is_connected():
            cursor = connection.cursor()
            upsert_query = """
            INSERT INTO ent_Catalog_ФизическиеЛица 
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
            ДатаУвольнения = VALUES(ДатаУвольнения);
            """
            cursor.executemany(upsert_query, records)
            connection.commit()
            print(f"✅ Успішно вставлено/оновлено {cursor.rowcount} записів у таблицю ent_Catalog_ФизическиеЛица.")
    except Error as e:
        print(f"❌ Помилка при роботі з MySQL: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Основна логіка виконання
def main():
    print("🔄 Отримання даних з API...")
    entries = fetch_data_from_api()
    records = []
    
    for entry in entries:
        records.append((
            entry['Ref_Key'], entry['DataVersion'], entry['DeletionMark'], entry['Parent_Key'], entry['IsFolder'],
            entry['Code'], entry['Description'], entry.get('ДатаРождения'), entry.get('Комментарий'),
            entry.get('Должность_Key'), entry.get('ИспользоватьГрафикРаботы'), entry.get('УДАЛИТЬРазрешитьВыбиратьБезПароля'),
            entry.get('УДАЛИТЬНаборПрав_Key'), entry.get('УДАЛИТЬВариантПечати'), entry.get('НеПоказыватьВГрафикеРаботы'),
            entry.get('SOVA_UDSВнешнийИдентификатор'), entry.get('ДатаПриема'), entry.get('ДатаУвольнения'),
            entry.get('Роль_Key'), entry.get('ДопРоль1_Key'), entry.get('ДопРоль2_Key'), entry.get('ДопРоль3_Key'),
            entry.get('МПК_ЗаписьРазрешена'), entry.get('МПК_ПредставлениеДолжности'), entry.get('МПК_ФотоСотрудника_Key'),
            entry.get('ID', ''), entry.get('ИНН'), entry['Predefined'], entry['PredefinedDataName']
        ))
    
    if records:
        print("📤 Завантаження даних у БД...")
        upsert_records_into_mysql(records)
    else:
        print("⚠️ Дані відсутні або API повернув порожній результат.")
    
if __name__ == "__main__":
    main()
