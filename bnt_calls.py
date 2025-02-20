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

# Конфігурація API
API_URL = os.getenv("BNT_URL")
API_KEY = os.getenv("BNT_KEY")
API_SECRET = os.getenv("BNT_SECRET")

# Конфігурація MySQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}

def get_calls(timestamp):
    payload = {
        "timestamp": timestamp,
        "key": API_KEY,
        "secret": API_SECRET
    }
    response = requests.post(API_URL, json=payload)
    return response.json() if response.status_code == 200 else None

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
        
        for call_id, call in call_data.get("callDetails", {}).items():
            start_time = convert_timestamp(call["startTime"])
            if start_time:
                cursor.execute(insert_query, (
                    call["companyID"], call["generalCallID"], start_time,
                    call["callType"], call["internalNumber"], call.get("internalAdditionalData", ""),
                    call["externalNumber"], call["waitsec"], call["billsec"],
                    call["disposition"], call["isNewCall"]
                ))
        
        connection.commit()
    except Error as e:
        print(f"Помилка MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    start_date = datetime.datetime(2024, 10, 15)
    end_date = datetime.datetime.now()
    
    while start_date <= end_date:
        timestamp = int(start_date.timestamp())
        print(f"Отримуємо дзвінки за {start_date.strftime('%Y-%m-%d')}")
        
        call_data = get_calls(timestamp)
        if call_data and call_data.get("status") == "success":
            save_to_db(call_data)
        else:
            print("Помилка отримання даних")
        
        time.sleep(6)  # Затримка між запитами
        start_date += datetime.timedelta(days=1)
