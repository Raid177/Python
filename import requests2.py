import requests
import mysql.connector
import time

# Параметри API
api_url = "https://enote.zoolux.clinic/09e599a6-2486-11e3-0983-08606e6953d2/odata/standard.odata/Catalog_ФизическиеЛица"
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
        INSERT INTO et_Catalog_ФизическиеЛица(
            Ref_Key, DataVersion, DeletionMark, Parent_Key, IsFolder, Code, Description,
            ДатаРождения, Комментарий, Должность_Key, ИспользоватьГрафикРаботы,
            УДАЛИТЬРазрешитьВыбиратьБезПароля, УДАЛИТЬНаборПрав_Key, УДАЛИТЬВариантПечати,
            НеПоказыватьВГрафикеРаботы, SOVA_UDSВнешнийИдентификатор, ДатаПриема, ДатаУвольнения,
            Роль_Key, ДопРоль1_Key, ДопРоль2_Key, ДопРоль3_Key, МПК_ЗаписьРазрешена,
            МПК_ПредставлениеДолжности, МПК_ФотоСотрудника_Key, ID, ИНН
        ) VALUES (
            %(Ref_Key)s, %(DataVersion)s, %(DeletionMark)s, %(Parent_Key)s, %(IsFolder)s,
            %(Code)s, %(Description)s, %(ДатаРождения)s, %(Комментарий)s, %(Должность_Key)s,
            %(ИспользоватьГрафикРаботы)s, %(УДАЛИТЬРазрешитьВыбиратьБезПароля)s,
            %(УДАЛИТЬНаборПрав_Key)s, %(УДАЛИТЬВариантПечати)s, %(НеПоказыватьВГрафикеРаботы)s,
            %(SOVA_UDSВнешнийИдентификатор)s, %(ДатаПриема)s, %(ДатаУвольнения)s,
            %(Роль_Key)s, %(ДопРоль1_Key)s, %(ДопРоль2_Key)s, %(ДопРоль3_Key)s,
            %(МПК_ЗаписьРазрешена)s, %(МПК_ПредставлениеДолжности)s, %(МПК_ФотоСотрудника_Key)s,
            %(ID)s, %(ИНН)s
        ) ON DUPLICATE KEY UPDATE
            DataVersion = VALUES(DataVersion),
            DeletionMark = VALUES(DeletionMark),
            Parent_Key = VALUES(Parent_Key),
            IsFolder = VALUES(IsFolder),
            Code = VALUES(Code),
            Description = VALUES(Description),
            ДатаРождения = VALUES(ДатаРождения),
            Комментарий = VALUES(Комментарий),
            Должность_Key = VALUES(Должность_Key),
            ИспользоватьГрафикРаботы = VALUES(ИспользоватьГрафикРаботы),
            УДАЛИТЬРазрешитьВыбиратьБезПароля = VALUES(УДАЛИТЬРазрешитьВыбиратьБезПароля),
            УДАЛИТЬНаборПрав_Key = VALUES(УДАЛИТЬНаборПрав_Key),
            УДАЛИТЬВариантПечати = VALUES(УДАЛИТЬВариантПечати),
            НеПоказыватьВГрафикеРаботы = VALUES(НеПоказыватьВГрафикеРаботы),
            SOVA_UDSВнешнийИдентификатор = VALUES(SOVA_UDSВнешнийИдентификатор),
            ДатаПриема = VALUES(ДатаПриема),
            ДатаУвольнения = VALUES(ДатаУвольнения),
            Роль_Key = VALUES(Роль_Key),
            ДопРоль1_Key = VALUES(ДопРоль1_Key),
            ДопРоль2_Key = VALUES(ДопРоль2_Key),
            ДопРоль3_Key = VALUES(ДопРоль3_Key),
            МПК_ЗаписьРазрешена = VALUES(МПК_ЗаписьРазрешена),
            МПК_ПредставлениеДолжности = VALUES(МПК_ПредставлениеДолжности),
            МПК_ФотоСотрудника_Key = VALUES(МПК_ФотоСотрудника_Key),
            ID = VALUES(ID),
            ИНН = VALUES(ИНН);
    """
    cursor.executemany(sql_query, records)

# Завантаження даних з API
def fetch_data():
    offset = 0
    limit = 500
    total_records = 0

    connection = connect_to_mysql()
    cursor = connection.cursor()

    while True:
        params = {
            "$top": limit,
            "$skip": offset,
            "$format": "json"
        }

        response = requests.get(api_url, auth=api_auth, params=params)

        if response.status_code != 200:
            print(f"Помилка при отриманні даних: {response.status_code} {response.text}")
            break

        data = response.json().get("value", [])

        if not data:
            print("Завантаження завершено.")
            break

        # Фільтрація полів, які потрібно зберігати в БД
        filtered_data = [
            {
                "Ref_Key": item.get("Ref_Key"),
                "DataVersion": item.get("DataVersion"),
                "DeletionMark": item.get("DeletionMark"),
                "Parent_Key": item.get("Parent_Key"),
                "IsFolder": item.get("IsFolder"),
                "Code": item.get("Code"),
                "Description": item.get("Description"),
                "ДатаРождения": item.get("ДатаРождения"),
                "Комментарий": item.get("Комментарий"),
                "Должность_Key": item.get("Должность_Key"),
                "ИспользоватьГрафикРаботы": item.get("ИспользоватьГрафикРаботы"),
                "УДАЛИТЬРазрешитьВыбиратьБезПароля": item.get("УДАЛИТЬРазрешитьВыбиратьБезПароля"),
                "УДАЛИТЬНаборПрав_Key": item.get("УДАЛИТЬНаборПрав_Key"),
                "УДАЛИТЬВариантПечати": item.get("УДАЛИТЬВариантПечати"),
                "НеПоказыватьВГрафикеРаботы": item.get("НеПоказыватьВГрафикеРаботы"),
                "SOVA_UDSВнешнийИдентификатор": item.get("SOVA_UDSВнешнийИдентификатор"),
                "ДатаПриема": item.get("ДатаПриема"),
                "ДатаУвольнения": item.get("ДатаУвольнения"),
                "Роль_Key": item.get("Роль_Key"),
                "ДопРоль1_Key": item.get("ДопРоль1_Key"),
                "ДопРоль2_Key": item.get("ДопРоль2_Key"),
                "ДопРоль3_Key": item.get("ДопРоль3_Key"),
                "МПК_ЗаписьРазрешена": item.get("МПК_ЗаписьРазрешена"),
                "МПК_ПредставлениеДолжности": item.get("МПК_ПредставлениеДолжности"),
                "МПК_ФотоСотрудника_Key": item.get("МПК_ФотоСотрудника_Key"),
                "ID": item.get("ID"),
                "ИНН": item.get("ИНН")
            } for item in data
        ]

        try:
            insert_records(filtered_data, cursor)
            connection.commit()
            total_records += len(filtered_data)
            print(f"Отримано {len(filtered_data)} записів, всього збережено: {total_records}")
        except mysql.connector.Error as e:
            print(f"Помилка при роботі з MySQL: {e}")
            connection.rollback()

        offset += limit
        time.sleep(3)

    cursor.close()
    connection.close()

if __name__ == "__main__":
    fetch_data()