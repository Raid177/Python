import os
import requests
import mysql.connector
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# 🧪 Завантаження налаштувань
load_dotenv()

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

PAGE_SIZE = 1000

# 🎯 Поля, які є в БД
ODATA_FIELDS = [
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

def get_records_from_odata():
    skip = 0
    while True:
        params = {
            "$format": "json",
            "$top": str(PAGE_SIZE),
            "$skip": str(skip),
            "$select": ",".join(ODATA_FIELDS)
        }

        response = requests.get(ODATA_URL, auth=ODATA_AUTH, params=params)
        print(f"🔗 {response.url}")
        if response.status_code != 200:
            print(f"❌ Помилка {response.status_code}: {response.text}")
            break

        data = response.json().get("value", [])
        if not data:
            break

        print(f"📦 Отримано {len(data)} записів (skip={skip})")
        yield from data

        if len(data) < PAGE_SIZE:
            break

        skip += PAGE_SIZE

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

    stats = {"inserted": 0, "updated": 0, "skipped": 0}

    for row in get_records_from_odata():
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
