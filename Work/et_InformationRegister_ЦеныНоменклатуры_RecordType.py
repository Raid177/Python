import os
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

# Завантаження змінних з .env
load_dotenv()

odata_url = os.getenv("ODATA_BASE_URL") + "InformationRegister_ЦеныНоменклатуры_RecordType"
odata_user = os.getenv("ODATA_USER")
odata_pass = os.getenv("ODATA_PASSWORD")

db_config = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "charset": "utf8mb4"
}

# Параметри
BATCH_SIZE = 1000
DEFAULT_START_DATE = datetime(2024, 8, 1)

# Підключення до БД
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Отримати останню дату з БД
cursor.execute("SELECT MAX(Period) FROM et_InformationRegister_ЦеныНоменклатуры_RecordType")
result = cursor.fetchone()
last_date = result[0] if result and result[0] else DEFAULT_START_DATE
start_date = (last_date - timedelta(days=14)).strftime('%Y-%m-%dT00:00:00')

print(f"Отримуємо дані за період з {start_date}")

received_total = 0
skip = 0

while True:
    params = {
        "$format": "json",
        "$orderby": "Period",
        "$top": BATCH_SIZE,
        "$skip": skip,
        "$filter": f"Period ge datetime'{start_date}'"
    }

    response = requests.get(odata_url, params=params, auth=(odata_user, odata_pass))
    response.raise_for_status()
    data = response.json().get('value', [])

    if not data:
        break

    print(f"Отримано рядків: {len(data)}")

    for row in data:
        cursor.execute("""
            INSERT INTO et_InformationRegister_ЦеныНоменклатуры_RecordType (
                Period, Recorder, Recorder_Type, LineNumber, Active,
                ТипЦен_Key, Номенклатура_Key, ЕдиницаИзмерения_Key, Валюта_Key, Цена,
                created_at, updated_at
            ) VALUES (
                %(Period)s, %(Recorder)s, %(Recorder_Type)s, %(LineNumber)s, %(Active)s,
                %(ТипЦен_Key)s, %(Номенклатура_Key)s, %(ЕдиницаИзмерения_Key)s, %(Валюта_Key)s, %(Цена)s,
                NOW(), NOW()
            )
            ON DUPLICATE KEY UPDATE
                Period = VALUES(Period),
                Recorder_Type = VALUES(Recorder_Type),
                Active = VALUES(Active),
                ТипЦен_Key = VALUES(ТипЦен_Key),
                Номенклатура_Key = VALUES(Номенклатура_Key),
                ЕдиницаИзмерения_Key = VALUES(ЕдиницаИзмерения_Key),
                Валюта_Key = VALUES(Валюта_Key),
                Цена = VALUES(Цена),
                updated_at = NOW()
        """, {
            "Period": row["Period"],
            "Recorder": row["Recorder"],
            "Recorder_Type": row["Recorder_Type"],
            "LineNumber": int(row["LineNumber"]),
            "Active": row["Active"],
            "ТипЦен_Key": row["ТипЦен_Key"],
            "Номенклатура_Key": row["Номенклатура_Key"],
            "ЕдиницаИзмерения_Key": row["ЕдиницаИзмерения_Key"],
            "Валюта_Key": row["Валюта_Key"],
            "Цена": float(row["Цена"])
        })

    conn.commit()
    received_total += len(data)
    skip += BATCH_SIZE
    time.sleep(1)

end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Отримано всього {received_total} рядків за період з {start_date} по {end_date}")

cursor.close()
conn.close()
