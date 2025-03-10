import requests
import mysql.connector
from mysql.connector import Error
import time

# Параметри API
base_url = "https://enote.zoolux.clinic/09e599a6-2486-11e3-0983-08606e6953d2/odata/standard.odata/AccumulationRegister_Зарплата"
api_auth = ('zooluxcab', 'mTuee0m5')  # Логін і пароль для API
batch_size = 2328  # Кількість записів за один запит
pause_duration = 2  # Перерва між запитами (у секундах)

# Параметри MySQL
mysql_host = "wealth0.mysql.tools"
mysql_user = "wealth0_raid"
mysql_password = "n5yM5ZT87z"
mysql_db = "wealth0_analytics"

# Функція для вставки даних у MySQL
def insert_records_into_mysql(records):
    try:
        connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_db
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Вставка даних у таблицю
            insert_query = """
            INSERT INTO ent_AccumulationRegister_Зарплата (
                Recorder, Recorder_Type, Period, LineNumber, Active, RecordType, Организация_Key, Сотрудник_Key, Сумма
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.executemany(insert_query, records)
            connection.commit()

            print(f"Успішно вставлено {cursor.rowcount} записів у таблицю.")
    except Error as e:
        print(f"Помилка при роботі з MySQL: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Функція для отримання даних з API
def fetch_data_from_api():
    records = []
    skip = 0  # Параметр пропуску записів
    while True:
        try:
            # Формуємо URL для отримання даних пачками
            api_url = f"{base_url}?$format=json&$orderby=Period&$top={batch_size}&$skip={skip}"
            response = requests.get(api_url, auth=api_auth)
            response.raise_for_status()  # Перевірка на помилки
            data = response.json()

            # Отримуємо значення записів
            batch = data.get('value', [])
            if not batch:
                break  # Якщо немає більше даних, виходимо з циклу

            # Обробляємо записи
            for item in batch:
                for record in item.get('RecordSet', []):
                    records.append((
                        item['Recorder'], 
                        item['Recorder_Type'], 
                        record['Period'], 
                        record['LineNumber'], 
                        record['Active'], 
                        record['RecordType'], 
                        record['Организация_Key'], 
                        record['Сотрудник_Key'], 
                        record['Сумма']
                    ))
            
            # Вставляємо зібрані записи в MySQL
            if records:
                insert_records_into_mysql(records)
                records.clear()  # Очищуємо список після вставки
            
            # Переходимо до наступної порції даних
            skip += batch_size
            print(f"Отримано {len(batch)} записів, пропущено {skip} записів.")
            
            # Перерва між запитами
            time.sleep(pause_duration)

        except requests.exceptions.RequestException as e:
            print(f"Помилка при отриманні даних з API: {e}")
            break

# Виконання скрипта
fetch_data_from_api()
