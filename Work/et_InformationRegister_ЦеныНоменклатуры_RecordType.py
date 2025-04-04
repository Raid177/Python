import os
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

# Завантаження .env
load_dotenv()

# Авторизація
ODATA_URL = os.getenv("ODATA_URL") + "InformationRegister_ЦеныНоменклатуры_RecordType"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "autocommit": True
}

BATCH_SIZE = 1000
DEFAULT_START_DATE = datetime(2024, 8, 1)

# Підключення до БД
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# Отримати останній Period
cursor.execute("SELECT MAX(Period) FROM et_InformationRegister_ЦеныНоменклатуры_RecordType")
result = cursor.fetchone()
last_date = result[0] if result and result[0] else DEFAULT_START_DATE
cutoff_date = last_date - timedelta(days=14)

print(f"Максимальна дата в БД: {last_date}")
print(f"Оброблятимемо лише записи з {cutoff_date}")

received_total = 0
written_total = 0
skip = 0

while True:
    params = {
        "$format": "json",
        "$orderby": "Period",
        "$top": BATCH_SIZE,
        "$skip": skip
    }

    response = requests.get(ODATA_URL, params=params, auth=(ODATA_USER, ODATA_PASSWORD))
    response.raise_for_status()
    data = response.json().get("value", [])

    if not data:
        break

    print(f"Отримано записів: {len(data)}")

    for row in data:
        period_dt = datetime.fromisoformat(row["Period"])

        if period_dt >= cutoff_date:
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
            written_total += 1

    received_total += len(data)
    skip += BATCH_SIZE
    time.sleep(1)

print(f"\n🔚 Завершено.")
print(f"Усього отримано з OData: {received_total} рядків")
print(f"Записано/оновлено в БД (за останні 14 днів): {written_total} рядків")

cursor.close()
conn.close()
