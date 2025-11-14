# /root/Python/E-Note/et_Catalog_Номенклатура.py
import os
import time
import requests
import mysql.connector
from pathlib import Path
from dotenv import load_dotenv

# === Константи ===
BATCH_SIZE = 1000
SLEEP_SECONDS = 1
REQ_TIMEOUT = 60

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

ODATA_BASE_URL = os.getenv('ODATA_URL').rstrip('/')
ODATA_USER = os.getenv('ODATA_USER')
ODATA_PASSWORD = os.getenv('ODATA_PASSWORD')

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "autocommit": True,
}

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()
session = requests.Session()
session.auth = (ODATA_USER, ODATA_PASSWORD)

db_fields = [
    'Ref_Key','DataVersion','DeletionMark','Parent_Key','IsFolder','Code','Description',
    'SOVA_UDSМаксимальныйПроцентОплатыБаллами','SOVA_UDSНеПрименятьСкидку','SOVA_UDSПроцентДополнительногоНачисления',
    'АналитикаПоЗарплате_Key','Артикул','Весовой','ВестиУчетПоСериям','Вид_Key','ВидНоменклатуры',
    'ЕдиницаБазовая_Key','ЕдиницаДляОтчетов_Key','ЕдиницаИнвентаризации_Key','ЕдиницаПоставок_Key','ЕдиницаРозницы_Key',
    'ЕдиницаФиксированная_Key','ЕдиницаХраненияОстатков_Key','ЕдиницаЦены_Key','ЗапрещенаРозничнаяТорговля','КодВнешнейБазы',
    'Predefined','PredefinedDataName'
]

def fetch_page(page_number: int):
    params = {
        "$format": "json",
        "$orderby": "Ref_Key",
        "$top": BATCH_SIZE,
        "$skip": page_number * BATCH_SIZE,
        "$select": ",".join(db_fields)
    }
    url = f"{ODATA_BASE_URL}/Catalog_Номенклатура"
    r = session.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.json().get('value', [])

def upsert_rows(entries):
    if not entries:
        return 0
    conn.ping(reconnect=True, attempts=2, delay=1)
    written = 0
    placeholders = ', '.join(['%s'] * len(db_fields))
    columns = ', '.join(db_fields)
    update_clause = ', '.join([f"{f} = VALUES({f})" for f in db_fields])

    for entry in entries:
        values = tuple(entry.get(field) for field in db_fields)
        cursor.execute(f"""
            INSERT INTO et_Catalog_Номенклатура ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """, values)
        written += 1
    return written

def main():
    page = 0
    total = 0

    while True:
        items = fetch_page(page)
        if not items:
            break
        total += len(items)
        print(f"Пагінація: {page + 1}, Отримано всього: {total}")
        upsert_rows(items)
        page += 1
        time.sleep(SLEEP_SECONDS)

    print("✅ Дані успішно перенесені в et_Catalog_Номенклатура")

if __name__ == "__main__":
    main()
