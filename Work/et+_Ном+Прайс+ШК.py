import os
import mysql.connector
from dotenv import load_dotenv

# Завантаження змінних середовища
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# Підключення до бази
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)
cursor = conn.cursor()

# Запит на оновлення таблиці
sql = """
REPLACE INTO `et+_Ном+Прайс+ШК` (
    Номенклатура_Key, Найменування, Артикул, Вид_Key, ЕдиницаРозницы_Key,
    Штрихкод, АктуальнаЦіна, ДатаЦіни
)
SELECT 
    nom.Ref_Key AS Номенклатура_Key,
    nom.Description AS Найменування,
    nom.Артикул,
    nom.Вид_Key,
    nom.ЕдиницаРозницы_Key,
    shc.Штрихкод,
    pr.Цена AS АктуальнаЦіна,
    pr.Period AS ДатаЦіни
FROM 
    et_Catalog_Номенклатура AS nom
INNER JOIN et_InformationRegister_Штрихкоды AS shc 
    ON nom.Ref_Key = shc.Номенклатура 
    AND nom.ЕдиницаРозницы_Key = shc.ЕдиницаИзмерения_Key
LEFT JOIN (
    SELECT p1.Номенклатура_Key, p1.ЕдиницаИзмерения_Key, p1.Цена, p1.Period
    FROM `et+_InformationRegister_ЦеныНоменклатуры_RecordType` AS p1
    INNER JOIN (
        SELECT Номенклатура_Key, ЕдиницаИзмерения_Key, MAX(Period) AS MaxPeriod
        FROM `et+_InformationRegister_ЦеныНоменклатуры_RecordType`
        WHERE ТипЦен_Key = 'fead93d8-4e84-11ef-83bb-2ae983d8a0f0'
        GROUP BY Номенклатура_Key, ЕдиницаИзмерения_Key
    ) AS latest
    ON p1.Номенклатура_Key = latest.Номенклатура_Key 
       AND p1.ЕдиницаИзмерения_Key = latest.ЕдиницаИзмерения_Key
       AND p1.Period = latest.MaxPeriod
    WHERE p1.ТипЦен_Key = 'fead93d8-4e84-11ef-83bb-2ae983d8a0f0'
) AS pr
    ON nom.Ref_Key = pr.Номенклатура_Key 
   AND nom.ЕдиницаРозницы_Key = pr.ЕдиницаИзмерения_Key
WHERE 
    nom.АналитикаПоЗарплате_Key = '4cf516b4-17bf-11e3-5888-08606e6953d2'
    AND nom.IsFolder = 0;
"""

cursor.execute(sql)
conn.commit()

print("✅ Дані оновлено в таблиці et+_Ном+Прайс+ШК")

cursor.close()
conn.close()
