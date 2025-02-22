import requests
import json
import time
import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Завантаження змінних з .env
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

# 🔹 Функція отримання дзвінків з API
def get_calls(start_time, stop_time):
    payload = {
        "startTime": start_time,
        "stopTime": stop_time,
        "key": API_KEY,
        "secret": API_SECRET
    }

    headers = {"Content-Type": "application/json"}
    print(f"📡 Запит до API: {API_URL}, Параметри: {payload}")

    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=10)
        print(f"🔍 Код відповіді API: {response.status_code}, Відповідь: {response.text[:500]}")

        return response.json() if response.status_code == 200 else None
    except requests.exceptions.RequestException as e:
        print(f"❌ Помилка запиту: {e}")
        return None

# 🔹 Конвертація timestamp у формат MySQL
def convert_timestamp(ts):
    try:
        return datetime.datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None

def save_to_db(call_data):
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
            companyID=VALUES(companyID), startTime=VALUES(startTime),
            callType=VALUES(callType), internalNumber=VALUES(internalNumber),
            internalAdditionalData=VALUES(internalAdditionalData),
            externalNumber=VALUES(externalNumber), waitsec=VALUES(waitsec),
            billsec=VALUES(billsec), disposition=VALUES(disposition),
            isNewCall=VALUES(isNewCall)
        """

        for call_id, call in call_data.get("callDetails", {}).items():  # 🔹 Додаємо .items() для розпакування ID
            start_time = convert_timestamp(call["startTime"])  # 🔹 Час дзвінка у нормальному форматі
            if start_time:
                cursor.execute(insert_query, (
                    call["companyID"], call["generalCallID"], start_time,
                    call["callType"], call["internalNumber"], call.get("internalAdditionalData", ""),
                    call["externalNumber"], call["waitsec"], call["billsec"],
                    call["disposition"], call["isNewCall"]
                ))

        connection.commit()
        print("✅ Дані збережені в MySQL")

    except Error as e:
        print(f"❌ Помилка MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# 🔹 Основний код: отримання дзвінків за 20-21 лютого 2025
if __name__ == "__main__":
    start_date = datetime.datetime(2025, 2, 20)
    end_date = datetime.datetime(2025, 2, 21, 23, 59, 59)

    while start_date <= end_date:
        start_time = int(start_date.timestamp())
        stop_time = start_time + 14400  # 4 години

        print(f"🔍 Отримуємо дзвінки за {start_date.strftime('%Y-%m-%d %H:%M:%S')}")

        call_data = get_calls(start_time, stop_time)
        if call_data and call_data.get("status") == "success":
            save_to_db(call_data)
        else:
            print("❌ Даних нема або помилка API")

        time.sleep(6)  # Чекаємо перед наступним запитом
        start_date += datetime.timedelta(hours=4)  # Рухаємось по 4 години
