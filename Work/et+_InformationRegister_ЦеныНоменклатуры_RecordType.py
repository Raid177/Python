import os
import pymysql
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Завантаження змінних з .env
load_dotenv()

# Параметри підключення
connection = pymysql.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_DATABASE'),
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

batch_size = 1000
total_inserted = 0

with connection:
    with connection.cursor() as cursor:
        # Отримати останню дату з нової таблиці
        cursor.execute("SELECT MAX(Period) AS last_date FROM `et+_InformationRegister_ЦеныНоменклатуры_RecordType`")
        result = cursor.fetchone()
        last_date = result['last_date'] or datetime(2024, 1, 1)
        from_date = last_date - timedelta(days=14)

        logging.info(f"Отримання даних з {from_date.strftime('%Y-%m-%d')}...")

        # Запит з джойнами та фільтрацією
        select_query = f"""
        SELECT 
            p.Period,
            p.Recorder,
            p.Recorder_Type,
            p.LineNumber,
            p.Active,
            p.ТипЦен_Key,
            p.Номенклатура_Key,
            p.ЕдиницаИзмерения_Key,
            p.Валюта_Key,
            p.Цена,
            CASE WHEN p.ЕдиницаИзмерения_Key = n.ЕдиницаХраненияОстатков_Key THEN 1 ELSE 0 END AS Is_БазоваяЕдиница,
            eu.Коэффициент,
            eu.Description AS Одиниця,
            eu_base.Description AS БазОДНазва
        FROM et_InformationRegister_ЦеныНоменклатуры_RecordType p
        LEFT JOIN et_Catalog_Номенклатура n ON p.Номенклатура_Key = n.Ref_Key
        LEFT JOIN et_Catalog_ЕдиницыИзмерения eu ON p.ЕдиницаИзмерения_Key = eu.Ref_Key
        LEFT JOIN et_Catalog_ЕдиницыИзмерения eu_base ON n.ЕдиницаХраненияОстатков_Key = eu_base.Ref_Key
        WHERE p.Period >= %s
        ORDER BY p.Period
        """

        cursor.execute(select_query, (from_date,))
        rows = cursor.fetchall()

        insert_query = """
        INSERT INTO `et+_InformationRegister_ЦеныНоменклатуры_RecordType` (
            Period, Recorder, Recorder_Type, LineNumber, Active,
            ТипЦен_Key, Номенклатура_Key, ЕдиницаИзмерения_Key, Валюта_Key, Цена,
            Is_БазоваяЕдиница, Коэффициент, Одиниця, БазОДНазва
        ) VALUES (
            %(Period)s, %(Recorder)s, %(Recorder_Type)s, %(LineNumber)s, %(Active)s,
            %(ТипЦен_Key)s, %(Номенклатура_Key)s, %(ЕдиницаИзмерения_Key)s, %(Валюта_Key)s, %(Цена)s,
            %(Is_БазоваяЕдиница)s, %(Коэффициент)s, %(Одиниця)s, %(БазОДНазва)s
        )
        ON DUPLICATE KEY UPDATE
            Active = VALUES(Active),
            Цена = VALUES(Цена),
            Is_БазоваяЕдиница = VALUES(Is_БазоваяЕдиница),
            Коэффициент = VALUES(Коэффициент),
            Одиниця = VALUES(Одиниця),
            БазОДНазва = VALUES(БазОДНазва),
            updated_at = CURRENT_TIMESTAMP
        """

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            for row in batch:
                cursor.execute(insert_query, row)
            connection.commit()
            total_inserted += len(batch)
            logging.info(f"Оброблено {total_inserted} записів...")

        logging.info(f"✅ Успішно завершено. Всього оброблено: {total_inserted} записів.")
