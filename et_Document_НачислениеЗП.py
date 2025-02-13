import requests
import mysql.connector
import time

# Параметри API
api_url = "https://enote.zoolux.clinic/09e599a6-2486-11e3-0983-08606e6953d2/odata/standard.odata/Document_НачислениеЗП"
api_auth = ("zooluxcab", "mTuee0m5")

# Параметри MySQL
mysql_host = "wealth0.mysql.tools"
mysql_user = "wealth0_raid"
mysql_password = "n5yM5ZT87z"
mysql_db = "wealth0_analytics"

# Підключення до MySQL
def connect_to_mysql():
    return mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db
    )

# Вставка даних у таблицю MySQL
def insert_records(records, cursor):
    sql_query = """
        INSERT INTO et_Document_НачислениеЗП (
            Ref_Key, DataVersion, DeletionMark, Number, Date, Posted,
            Организация_Key, Ответственный_Key, СуммаДокумента, Комментарий,
            LineNumber, Сотрудник_Key, Сумма, Статья_Key, Комментарий_Статья
        ) VALUES (
            %(Ref_Key)s, %(DataVersion)s, %(DeletionMark)s, %(Number)s, %(Date)s, %(Posted)s,
            %(Организация_Key)s, %(Ответственный_Key)s, %(СуммаДокумента)s, %(Комментарий)s,
            %(LineNumber)s, %(Сотрудник_Key)s, %(Сумма)s, %(Статья_Key)s, %(Комментарий_Статья)s
        ) ON DUPLICATE KEY UPDATE
            DataVersion = VALUES(DataVersion),
            DeletionMark = VALUES(DeletionMark),
            Number = VALUES(Number),
            Date = VALUES(Date),
            Posted = VALUES(Posted),
            Организация_Key = VALUES(Организация_Key),
            Ответственный_Key = VALUES(Ответственный_Key),
            СуммаДокумента = VALUES(СуммаДокумента),
            Комментарий = VALUES(Комментарий),
            LineNumber = VALUES(LineNumber),
            Сотрудник_Key = VALUES(Сотрудник_Key),
            Сумма = VALUES(Сумма),
            Статья_Key = VALUES(Статья_Key),
            Комментарий_Статья = VALUES(Комментарий_Статья);
    """
    cursor.executemany(sql_query, records)

# Завантаження даних з API
def fetch_data():
    offset = 0
    limit = 56
    total_records = 0

    connection = connect_to_mysql()
    cursor = connection.cursor()

    while True:
        params = {
            "$top": limit,
            "$skip": offset,
            "$format": "json",
            "$orderby": "Date desc"

        }

        response = requests.get(api_url, auth=api_auth, params=params)

        if response.status_code != 200:
            print(f"Помилка при отриманні даних: {response.status_code} {response.text}")
            break

        data = response.json().get("value", [])

        if not data:
            print("Завантаження завершено.")
            break

        # Формування списку записів для вставки
        records = []
        for item in data:
            base_record = {
                "Ref_Key": item.get("Ref_Key"),
                "DataVersion": item.get("DataVersion"),
                "DeletionMark": item.get("DeletionMark"),
                "Number": item.get("Number"),
                "Date": item.get("Date"),
                "Posted": item.get("Posted"),
                "Организация_Key": item.get("Организация_Key"),
                "Ответственный_Key": item.get("Ответственный_Key"),
                "СуммаДокумента": item.get("СуммаДокумента"),
                "Комментарий": item.get("Комментарий"),
            }

            # Додавання даних про співробітників
            for employee in item.get("Сотрудники", []):
                record = base_record.copy()
                record.update({
                    "LineNumber": employee.get("LineNumber"),
                    "Сотрудник_Key": employee.get("Сотрудник_Key"),
                    "Сумма": employee.get("Сумма"),
                    "Статья_Key": employee.get("Статья_Key"),
                    "Комментарий_Статья": employee.get("Комментарий"),
                })
                records.append(record)

        try:
            insert_records(records, cursor)
            connection.commit()
            total_records += len(records)
            print(f"Отримано {len(records)} записів, всього збережено: {total_records}")
        except mysql.connector.Error as e:
            print(f"Помилка при роботі з MySQL: {e}")
            connection.rollback()

        offset += limit
        time.sleep(3)

    cursor.close()
    connection.close()

if __name__ == "__main__":
    fetch_data()
