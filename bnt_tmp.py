import requests
import json
import time
import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# 🔹 Завантаження змінних з .env
load_dotenv()

# 🔹 API Binotel
API_URL = "https://api.binotel.com/api/4.0/stats/list-of-calls-for-period.json"
API_KEY = os.getenv("BNT_KEY")
API_SECRET = os.getenv("BNT_SECRET")

# 🔹 Конфігурація MySQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}

# 🔹 Отримуємо останню дату в БД
def get_last_date_from_db():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(startTime) FROM bnt_calls")
        last_date = cursor.fetchone()[0]
        return last_date.date() - datetime.timedelta(days=1) if last_date else datetime.date.today()
    except Error as e:
        print(f"❌ Помилка MySQL: {e}")
        return datetime.date.today()
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# 🔹 Функція отримання дзвінків з API
def get_calls(start_time, stop_time):
    payload = {
        "startTime": int(start_time),
        "stopTime": int(stop_time),
        "key": API_KEY,
        "secret": API_SECRET
    }
    
    print(f"📡 Відправляємо запит: {payload}")
    try:
        response = requests.post(API_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=10)
        print(f"🔹 Код відповіді API: {response.status_code}")
        return response.json() if response.status_code == 200 else None
    except requests.exceptions.RequestException as e:
        print(f"❌ Помилка запиту: {e}")
        return None

# 🔹 Збереження дзвінків у БД
def save_to_db(call_data):
    if not isinstance(call_data, dict) or "callDetails" not in call_data:
        print(f"⚠️ Неправильна структура відповіді API: {json.dumps(call_data, indent=4, ensure_ascii=False)}")
        return 0

    call_details = call_data.get("callDetails", {})
    
    if not isinstance(call_details, dict):
        print(f"❌ Помилка: callDetails не є словником! callDetails={call_details}")
        return 0

    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO bnt_calls (
            companyID, generalCallID, startTime, callType, internalNumber,
            internalAdditionalData, externalNumber, waitsec, billsec,
            disposition, isNewCall
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            startTime=VALUES(startTime), callType=VALUES(callType),
            internalNumber=VALUES(internalNumber), internalAdditionalData=VALUES(internalAdditionalData),
            externalNumber=VALUES(externalNumber), waitsec=VALUES(waitsec),
            billsec=VALUES(billsec), disposition=VALUES(disposition),
            isNewCall=VALUES(isNewCall)
        """

        count = 0
        for call_id, call in call_details.items():
            try:
                start_time = int(call["startTime"])  # Переконуємося, що це int
                
                cursor.execute(insert_query, (
                    call["companyID"], call["generalCallID"], datetime.datetime.fromtimestamp(start_time),
                    call["callType"], call["internalNumber"], call.get("internalAdditionalData", ""),
                    call["externalNumber"], call["waitsec"], call["billsec"],
                    call["disposition"], call["isNewCall"]
                ))
                count += 1
            except KeyError as e:
                print(f"⚠️ Пропущено запис {call_id}: відсутній ключ {e}")
            except ValueError as e:
                print(f"❌ Помилка конвертації {call_id}: {e}")

        connection.commit()
        return count
    except Error as e:
        print(f"❌ Помилка MySQL: {e}")
        return 0
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# 🔹 Основний процес
if __name__ == "__main__":
    start_date = get_last_date_from_db()
    end_date = datetime.date.today()
    print(f"🚀 Отримуємо дзвінки з {start_date} до {end_date}")
    
    while start_date <= end_date:
        start_timestamp = int(time.mktime(start_date.timetuple()))
        stop_timestamp = int(time.mktime((start_date + datetime.timedelta(days=1)).timetuple()))
        
        call_data = get_calls(start_timestamp, stop_timestamp)
        if call_data:
            saved_records = save_to_db(call_data)
            print(f"📅 Дані за {start_date} отримано в кількості {saved_records} записів")
        else:
            print(f"⚠️ Немає даних за {start_date}")
        
        start_date += datetime.timedelta(days=1)
        time.sleep(6)
