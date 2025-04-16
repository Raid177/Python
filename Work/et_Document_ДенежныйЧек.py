import os
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

# Авторизація
ODATA_URL = os.getenv("ODATA_URL") + "Document_ДенежныйЧек"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")
ODATA_AUTH = HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "autocommit": True
}

PAGE_SIZE = 1
START_DATE_FALLBACK = datetime(2024, 7, 28)

ODATA_FIELDS = [  # всі поля з таблиці, окрім created_at, updated_at
    "Ref_Key", "DataVersion", "DeletionMark", "Number", "Date", "Posted",
    "Валюта_Key", "ВидДвижения", "ВидОплатыБезнал_Key", "ВидСкидки_Key",
    "ДенежныйСчет", "ДенежныйСчет_Type", "ДенежныйСчетБезнал_Key", "ДенежныйСчетКредит_Key",
    "ДисконтнаяКарточка_Key", "Заявка_Key", "Карточка_Key", "КассирИНН", "КассирФИО",
    "КодАвторизации", "Комментарий", "Кратность", "Курс", "МДЛПИД", "НаправлениеДвижения",
    "НеПередаватьКассира", "НомерПлатежнойКарты", "НомерЧекаЭТ", "Объект", "Объект_Type",
    "ОкруглятьИтогЧека", "Организация_Key", "Основание", "Основание_Type",
    "Ответственный_Key", "ОтправлятьEmail", "ОтправлятьСМС", "Подотчет",
    "Подразделение_Key", "ПроцентРучнойСкидкиНаценки", "Сдача", "СистемаНалогообложения",
    "Скидка", "СпособОкругленияИтогаЧека", "СсылочныйНомер", "СсылочныйНомерОснования",
    "Сумма", "СуммаБезнал", "СуммаКорр", "СуммаКредит", "СуммаПлатежаБонусы",
    "СуммаТорговойУступки", "УдалитьМеждуОрганизациями", "УдалитьОрганизацияПолучатель_Key",
    "УИДПлатежа", "УИДПлатежа1С", "УказанныйEmail", "УказанныйТелефон",
    "ФискальныйНомерЧека", "Электронно", "Инвойс_Key", "MistyLoyalty_ПараметрыОперации_Type",
    "MistyLoyalty_ПараметрыОперации_Base64Data", "ЭквайрИД", "ТерминалИД",
    "ПлатежнаяСистемаЭТ", "MistyLoyaltyOperationID", "СуммаВключаетНДС",
    "ДокументБезНДС", "ЭквайрНаименование", "ДругоеСредствоОплаты_Key",
    "ДенежныйСчетБезналДСО_Key", "СуммаБезналДСО", "Проверен"
]

def get_last_date_from_db(cursor):
    cursor.execute("SELECT MAX(`Date`) FROM et_Document_ДенежныйЧек")
    result = cursor.fetchone()
    return result[0] if result and result[0] else None

def get_records_from_odata(start_date):
    while True:
        formatted_date = start_date.strftime("%Y-%m-%dT%H:%M")
        params = {
            "$format": "json",
            "$orderby": "Date",
            "$top": str(PAGE_SIZE),
            "$filter": f"cast(Date,'Edm.DateTime') ge {formatted_date}",

            "$select": ",".join(ODATA_FIELDS)
        }

        response = requests.get(ODATA_URL, auth=ODATA_AUTH, params=params)
        if response.status_code != 200:
            print(f"❌ Помилка {response.status_code}: {response.text}")
            break

        data = response.json().get("value", [])
        if not data:
            break

        dates = [d["Date"] for d in data if "Date" in d]
        if dates:
            d_from = min(dates)
            d_to = max(dates)
            print(f"📦 Отримано {len(data)} записів з {d_from} по {d_to}")
        else:
            print(f"📦 Отримано {len(data)} записів (дата не визначена)")

        yield from data

        if len(data) < PAGE_SIZE:
            break

        start_date = datetime.fromisoformat(dates[-1]) + timedelta(seconds=1)

def insert_or_update(cursor, row):
    ref_key = row["Ref_Key"]
    data_version = row["DataVersion"]

    cursor.execute("SELECT DataVersion FROM et_Document_ДенежныйЧек WHERE Ref_Key = %s", (ref_key,))
    existing = cursor.fetchone()

    if existing:
        if existing[0] != data_version:
            update_record(cursor, row)
            return "updated"
        return "skipped"
    else:
        insert_record(cursor, row)
        return "inserted"

def insert_record(cursor, row):
    fields = ", ".join(row.keys()) + ", created_at, updated_at"
    placeholders = ", ".join(["%s"] * len(row)) + ", NOW(), NOW()"
    values = tuple(row.values())
    cursor.execute(
        f"INSERT INTO et_Document_ДенежныйЧек ({fields}) VALUES ({placeholders})", values
    )

def update_record(cursor, row):
    assignments = ", ".join([f"{key} = %s" for key in row.keys()])
    values = tuple(row.values()) + (row["Ref_Key"],)
    cursor.execute(
        f"UPDATE et_Document_ДенежныйЧек SET {assignments}, updated_at = NOW() WHERE Ref_Key = %s", values
    )

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    last_date = get_last_date_from_db(cursor)
    if last_date:
        date_from = last_date - timedelta(days=15)
    else:
        date_from = START_DATE_FALLBACK

    stats = {"inserted": 0, "updated": 0, "skipped": 0}

    for row in get_records_from_odata(date_from):
        result = insert_or_update(cursor, row)
        stats[result] += 1

    print("\n✅ Завершено:")
    print(f"➕ Додано: {stats['inserted']}")
    print(f"✏️  Оновлено: {stats['updated']}")
    print(f"⏭️  Пропущено без змін: {stats['skipped']}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
