import os
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

# Авторизація
ODATA_URL = os.getenv("ODATA_URL").rstrip("/")
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

ODATA_ENTITY = "Document_РозничныйЧек"
MYSQL_TABLE = "et_Document_РозничныйЧек"

# Поля, які використовуємо (всі з БД, крім created_at, updated_at)
odata_fields = [
    "Ref_Key", "DataVersion", "DeletionMark", "Number", "Date", "Posted", "ВидОперации", "ДенежныйСчет_Key",
    "ДенежныйСчетБезнал_Key", "ДенежныйСчетКредит_Key", "ДисконтнаяКарточка_Key", "КассирИНН", "КассирФИО",
    "КассоваяСмена_Key", "КодАвторизации", "Комментарий", "КорректировкаПоЛечению", "МДЛПИД", "НеПередаватьКассира",
    "НомерПлатежнойКарты", "НомерЧекаЭТ", "ОкруглятьИтогЧека", "Организация_Key", "Основание_Key", "Ответственный_Key",
    "ОтправлятьEmail", "ОтправлятьСМС", "Подразделение_Key", "Сдача", "СистемаНалогообложения", "Состояние",
    "СпособОкругленияИтогаЧека", "СсылочныйНомер", "СуммаДокумента", "СуммаОплатыБезнал", "СуммаОплатыБонусами",
    "СуммаОплатыКредитом", "СуммаОплатыНал", "СуммаТорговойУступки", "ТипЦен_Key", "УказанныйEmail", "УказанныйТелефон",
    "ФискальныйНомерЧека", "Электронно", "MistyLoyalty_ПараметрыОперации_Type", "MistyLoyalty_ПараметрыОперации_Base64Data",
    "ЭквайрИД", "ТерминалИД", "ПлатежнаяСистемаЭТ", "СсылочныйНомерОснования", "MistyLoyaltyOperationID",
    "СуммаВключаетНДС", "ДокументБезНДС", "ЭквайрНаименование", "ДругоеСредствоОплаты_Key", "Контрагент_Key",
    "Карточка_Key", "ДенежныйСчетБезналДСО_Key", "СуммаБезналДСО", "Проверен"
]

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE
    )

def get_last_date_from_db():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(`Date`) FROM `{MYSQL_TABLE}`")
    result = cursor.fetchone()[0]
    conn.close()
    if result:
        return result - timedelta(days=15)
    else:
        return datetime(2024, 7, 1)

def fetch_data(start_date, skip):
    filter_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    select_fields = ",".join(odata_fields)
    url = (
        f"{ODATA_URL}/{ODATA_ENTITY}"
        f"?$format=json&$orderby=Date asc&$top=1000&$skip={skip}"
        f"&$filter=Date ge datetime'{filter_date}'"
        f"&$select={select_fields}"
    )
    response = requests.get(url, auth=(ODATA_USER, ODATA_PASSWORD))
    response.raise_for_status()
    return response.json().get("value", [])

def insert_or_update_record(record, cursor):
    ref_key = record["Ref_Key"]
    dataversion = record.get("DataVersion", "")
    cursor.execute(
        f"SELECT `DataVersion` FROM `{MYSQL_TABLE}` WHERE `Ref_Key` = %s",
        (ref_key,)
    )
    result = cursor.fetchone()
    if result:
        if result[0] != dataversion:
            placeholders = ", ".join([f"`{k}` = %s" for k in record])
            sql = f"UPDATE `{MYSQL_TABLE}` SET {placeholders} WHERE `Ref_Key` = %s"
            cursor.execute(sql, list(record.values()) + [ref_key])
            return "updated"
        else:
            return "skipped"
    else:
        fields = ", ".join(f"`{k}`" for k in record)
        placeholders = ", ".join(["%s"] * len(record))
        sql = f"INSERT INTO `{MYSQL_TABLE}` ({fields}) VALUES ({placeholders})"
        cursor.execute(sql, list(record.values()))
        return "inserted"

def main():
    print(f"🚀 Старт завантаження {ODATA_ENTITY}")
    start_date = get_last_date_from_db()
    print(f"📅 Починаємо з дати: {start_date}")

    conn = connect_db()
    cursor = conn.cursor()
    skip = 0
    total_inserted = total_updated = total_skipped = 0

    while True:
        data = fetch_data(start_date, skip)
        if not data:
            break

        inserted = updated = skipped = 0
        dates = [d["Date"] for d in data if "Date" in d]
        min_date = min(dates)
        max_date = max(dates)

        for item in data:
            record = {k: item.get(k) for k in odata_fields}
            status = insert_or_update_record(record, cursor)
            if status == "inserted":
                inserted += 1
            elif status == "updated":
                updated += 1
            elif status == "skipped":
                skipped += 1

        conn.commit()

        print(f"📦 Отримано {len(data)} записів: з {min_date} по {max_date} | ➕ {inserted} 🔁 {updated} ⏭️ {skipped}")

        total_inserted += inserted
        total_updated += updated
        total_skipped += skipped
        skip += 1000

    conn.close()
    print(f"✅ Завершено. Всього ➕ {total_inserted} 🔁 {total_updated} ⏭️ {total_skipped}")

if __name__ == "__main__":
    main()
