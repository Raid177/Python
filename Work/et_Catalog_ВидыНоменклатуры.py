import requests
import pymysql
import time
import os
from dotenv import load_dotenv

# Завантаження змінних середовища
load_dotenv()

# Налаштування підключення до БД
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "cursorclass": pymysql.cursors.DictCursor
}

# Налаштування OData
ODATA_URL = os.getenv("ODATA_URL") + "Catalog_ВидыНоменклатуры"
ODATA_AUTH = (os.getenv("ODATA_USER"), os.getenv("ODATA_PASSWORD"))

BATCH_SIZE = 1000  # Кількість записів за один запит
SLEEP_TIME = 1  # Пауза між запитами


def fetch_odata(skip=0):
    """Отримання даних з OData."""
    params = {"$top": BATCH_SIZE, "$skip": skip, "$orderby": "Ref_Key", "$format": "json"}
    response = requests.get(ODATA_URL, auth=ODATA_AUTH, params=params)
    if response.status_code == 200:
        try:
            return response.json().get("value", [])
        except requests.exceptions.JSONDecodeError:
            print(f"Помилка розбору JSON: {response.text}")
            return []
    else:
        print(f"Помилка запиту OData: {response.status_code}, Відповідь: {response.text}")
        return []


def sync_data():
    """Синхронізація даних з OData в MariaDB."""
    connection = pymysql.connect(**DB_CONFIG)
    cursor = connection.cursor()
    skip = 0
    
    while True:
        records = fetch_odata(skip)
        if not records:
            break
        
        added = 0
        updated = 0
        total_received = len(records)
        
        for record in records:
            cursor.execute("SELECT DataVersion FROM et_Catalog_ВидыНоменклатуры WHERE Ref_Key = %s", (record["Ref_Key"],))
            existing = cursor.fetchone()
            
            if existing:
                if existing["DataVersion"] != record["DataVersion"]:
                    cursor.execute("""
                        UPDATE et_Catalog_ВидыНоменклатуры SET
                        DataVersion = %s, DeletionMark = %s, Parent_Key = %s, IsFolder = %s, Code = %s,
                        Description = %s, Наценка = %s, ID = %s, Predefined = %s, PredefinedDataName = %s,
                        updated_at = NOW()
                        WHERE Ref_Key = %s
                    """, (
                        record["DataVersion"], record["DeletionMark"], record["Parent_Key"], record["IsFolder"], record["Code"],
                        record["Description"], record["Наценка"], record["ID"], record["Predefined"], record["PredefinedDataName"],
                        record["Ref_Key"]
                    ))
                    updated += 1
            else:
                cursor.execute("""
                    INSERT INTO et_Catalog_ВидыНоменклатуры (
                        Ref_Key, DataVersion, DeletionMark, Parent_Key, IsFolder, Code, Description, Наценка, ID, Predefined, PredefinedDataName,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """, (
                    record["Ref_Key"], record["DataVersion"], record["DeletionMark"], record["Parent_Key"], record["IsFolder"],
                    record["Code"], record["Description"], record["Наценка"], record["ID"], record["Predefined"], record["PredefinedDataName"]
                ))
                added += 1
        
        connection.commit()
        print(f"Отримано записів: {total_received}")
        print(f"Оновлено записів: {updated}")
        print(f"Додано записів: {added}")
        
        skip += BATCH_SIZE
        time.sleep(SLEEP_TIME)
    
    cursor.close()
    connection.close()


if __name__ == "__main__":
    sync_data()