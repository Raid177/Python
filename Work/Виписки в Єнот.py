import requests
import pymysql
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dotenv import load_dotenv
import os

# Завантаження змінних з .env
load_dotenv()

# Параметри підключення до MariaDB
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_DATABASE")

# Параметри підключення до OData (Єнот)
ODATA_URL_BASE = os.getenv("ODATA_URL")
ODATA_TABLE = "Document_ДенежныйЧек"  # Назва таблиці
ODATA_URL_CREATE = f"{ODATA_URL_BASE}{ODATA_TABLE}"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}

# Підключення до БД
connection = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    cursorclass=pymysql.cursors.DictCursor
)

seen_errors = set()

try:
    with connection.cursor() as cursor:
        # Отримуємо всі транзакції, де TRANTYPE = 'D' і enote_ref IS NULL, сортуємо за DATE_TIME_DAT_OD_TIM_P
        cursor.execute("""
            SELECT * FROM bnk_trazact_prvt_ekv
            WHERE TRANTYPE = 'D' AND enote_ref IS NULL
            ORDER BY DATE_TIME_DAT_OD_TIM_P ASC
        """)
        transactions = cursor.fetchall()

        for transaction in transactions:
            num_doc = transaction["NUM_DOC"]
            date_time = transaction["DATE_TIME_DAT_OD_TIM_P"]
            aut_cntr_mfo = transaction["AUT_CNTR_MFO"]
            trantype = transaction["TRANTYPE"]
            aut_my_acc = transaction["AUT_MY_ACC"]
            aut_cntr_crf = transaction["AUT_CNTR_CRF"]
            sum_value = transaction["SUM"]
            osnd_text = transaction["OSND"]
            aut_cntr_nam = transaction["AUT_CNTR_NAM"]
            dat_od = transaction["DAT_OD"]
            enote_check = transaction["enote_check"]

            # Шукаємо Ref_Key у et_Catalog_ДенежныеСчета за НомерСчета
            cursor.execute("""
                SELECT Ref_Key FROM et_Catalog_ДенежныеСчета
                WHERE НомерСчета = %s
            """, (aut_my_acc,))
            account_result = cursor.fetchone()

            if not account_result:
                cursor.execute("""
                    UPDATE bnk_trazact_prvt_ekv
                    SET enote_check = 'Err ДенСчет'
                    WHERE NUM_DOC = %s AND DATE_TIME_DAT_OD_TIM_P = %s AND AUT_CNTR_MFO = %s AND TRANTYPE = %s
                """, (num_doc, date_time, aut_cntr_mfo, trantype))
                connection.commit()
                continue
            
            account_ref_key = account_result["Ref_Key"]

            # Шукаємо Ref_Key у et_counterparties
            cursor.execute("""
                SELECT Ref_Key FROM et_counterparties
                WHERE ЕДРПОУ = %s
            """, (aut_cntr_crf,))
            counterpart_result = cursor.fetchone()

            if not counterpart_result:
                if enote_check is None:
                    error_key = (aut_cntr_nam, osnd_text, dat_od)
                    if error_key not in seen_errors:
                        print(f"Контрагент '{aut_cntr_nam}', Платіж '{osnd_text}' не знайдено. Дата: {dat_od}")
                        seen_errors.add(error_key)
                cursor.execute("""
                    UPDATE bnk_trazact_prvt_ekv
                    SET enote_check = 'Err Объект'
                    WHERE NUM_DOC = %s AND DATE_TIME_DAT_OD_TIM_P = %s AND AUT_CNTR_MFO = %s AND TRANTYPE = %s
                """, (num_doc, date_time, aut_cntr_mfo, trantype))
                connection.commit()
                continue
            
            counterpart_ref_key = counterpart_result["Ref_Key"]
            error_fixed = enote_check is not None

            # Формуємо дані для створення чека
            data_create = {
                "Date": date_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "Posted": True,
                "Валюта_Key": "da1cdf6a-4e84-11ef-83bb-2ae983d8a0f0",
                "ВидДвижения": "РасчетыСПоставщиками",
                "ДенежныйСчет": account_ref_key,
                "ДенежныйСчетБезнал_Key": account_ref_key,
                "ДенежныйСчет_Type": "StandardODATA.Catalog_ДенежныеСчета",
                "Комментарий": f"Racoon_BankTransaction, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{osnd_text}",
                "Кратность": "1",
                "Курс": 1,
                "НаправлениеДвижения": "Расход",
                "Объект": counterpart_ref_key,
                "Объект_Type": "StandardODATA.Catalog_Контрагенты",
                "Организация_Key": "e3e20bc4-4e84-11ef-83bb-2ae983d8a0f0",
                "Ответственный_Key": "0b3e903a-eee3-11ef-9ac5-2ae983d8a0f0",
                "Подразделение_Key": "7f5078ca-4dfe-11ef-978c-2ae983d8a0f0",
                "Сумма": 0,
                "СуммаБезнал": float(sum_value)
            }

            # Відправляємо запит на створення документа в 1С (Єнот)
            response_create = requests.post(
                ODATA_URL_CREATE,
                headers=headers,
                auth=HTTPBasicAuth(ODATA_USER, ODATA_PASSWORD),
                json=data_create
            )

            if response_create.status_code == 201:
                response_data_create = response_create.json()
                enote_ref = response_data_create.get("Ref_Key")
                enote_check = response_data_create.get("Number")
                msg = f"Чек створено: {enote_check}, Дата: {dat_od}"
                if error_fixed:
                    msg += " (Помилку виправлено!)"
                print(msg)

                # Оновлюємо запис у БД
                cursor.execute("""
                    UPDATE bnk_trazact_prvt_ekv
                    SET enote_ref = %s, enote_check = %s
                    WHERE NUM_DOC = %s AND DATE_TIME_DAT_OD_TIM_P = %s AND AUT_CNTR_MFO = %s AND TRANTYPE = %s
                """, (enote_ref, enote_check, num_doc, date_time, aut_cntr_mfo, trantype))
                connection.commit()
            else:
                print(f"Помилка {response_create.status_code}: {response_create.text}")

finally:
    connection.close()
