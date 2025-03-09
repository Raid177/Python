import requests
import json
import time
import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()

API_URL = "https://api.binotel.com/api/4.0/stats/list-of-calls-for-period.json"
API_KEY = os.getenv("BNT_KEY")
API_SECRET = os.getenv("BNT_SECRET")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}

def get_last_date_from_db():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(startTime) FROM bnt_calls")
        last_date = cursor.fetchone()[0]
        return last_date.date() - datetime.timedelta(days=1) if last_date else datetime.date.today()
    except:
        return datetime.date.today()
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def get_calls(start_time, stop_time):
    payload = {"startTime": int(start_time), "stopTime": int(stop_time), "key": API_KEY, "secret": API_SECRET}
    try:
        response = requests.post(API_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=10)
        return response.json() if response.status_code == 200 else None
    except:
        return None

def save_to_db(call_data):
    if not isinstance(call_data, dict) or "callDetails" not in call_data:
        return 0

    call_details = call_data.get("callDetails", {})
    if not isinstance(call_details, dict):
        return 0

    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO bnt_calls (
            companyID, generalCallID, startTime, callType, internalNumber,
            internalAdditionalData, externalNumber, waitsec, billsec,
            disposition, isNewCall, pbxNumber
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            startTime=VALUES(startTime), callType=VALUES(callType),
            internalNumber=VALUES(internalNumber), internalAdditionalData=VALUES(internalAdditionalData),
            externalNumber=VALUES(externalNumber), waitsec=VALUES(waitsec),
            billsec=VALUES(billsec), disposition=VALUES(disposition),
            isNewCall=VALUES(isNewCall), pbxNumber=VALUES(pbxNumber)
        """

        count = 0
        for call in call_details.values():
            start_time = int(call["startTime"])
            pbx_number = call.get("pbxNumberData", {}).get("number", None)  # Додаємо поле pbxNumber
            cursor.execute(insert_query, (
                call["companyID"], call["generalCallID"], datetime.datetime.fromtimestamp(start_time),
                call["callType"], call["internalNumber"], call.get("internalAdditionalData", ""),
                call["externalNumber"], call["waitsec"], call["billsec"],
                call["disposition"], call["isNewCall"], pbx_number
            ))
            count += 1

        connection.commit()
        return count
    except:
        return 0
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    start_date = get_last_date_from_db()
    end_date = datetime.date.today()
    
    while start_date <= end_date:
        start_timestamp = int(time.mktime(start_date.timetuple()))
        stop_timestamp = int(time.mktime((start_date + datetime.timedelta(days=1)).timetuple()))
        
        call_data = get_calls(start_timestamp, stop_timestamp)
        saved_records = save_to_db(call_data) if call_data else 0
        print(f"\U0001F4C5 {start_date}: {saved_records} записів")
        
        start_date += datetime.timedelta(days=1)
        time.sleep(6)
