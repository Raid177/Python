#Беремо з файлу дані злиті з єноту Избранное - Продажі (PBI)... і заливаємо в БД

import pymysql
import pandas as pd
from datetime import datetime

# Параметри підключення до БД
mysql_host = "wealth0.mysql.tools"
mysql_user = "wealth0_raid"
mysql_password = "n5yM5ZT87z"
mysql_db = "wealth0_analytics"

# Файл для обробки
file_path = r"C:\test.txt"

# Читаємо файл, пропускаючи перші 3 рядки та останній рядок
with open(file_path, "r", encoding="utf-8") as file:
    lines = file.readlines()[4:-1]

# Обробляємо кожен рядок
data = []
for line in lines:
    cols = line.strip().split("\t")  # Табуляція як роздільник
    
    if len(cols) < 17:  # Новий файл має 17 колонок
        continue  # Пропускаємо рядки з неповними даними

    # 🕒 Конвертація дати
    raw_datetime = cols[1].split(";")[0].strip()  # Беремо першу частину другого стовпця
    datatime = datetime.strptime(raw_datetime, "%d.%m.%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

    # 🔍 Інші поля
    type_reg = cols[1].split(";")[1].strip() if ";" in cols[1] else ""  # Другу частину як тип реєстрації
    nom_description = cols[2].strip()
    nom_code = cols[3].strip()
    nom_type = cols[4].strip()  # НОВИЙ СТОВПЕЦЬ
    nom_analssalary = cols[5].strip()
    nom_analsale = cols[6].strip()
    nazn_description = cols[7].strip()
    nazn_code = cols[8].strip()
    isp_description = cols[9].strip()
    isp_code = cols[10].strip()
    posted = cols[11].strip()
    
    # 🔢 Числові поля (з комою -> у крапку)
    quant_vytracheno = cols[12].replace(",", ".").strip()
    quant_sale = cols[13].replace(",", ".").strip()
    price = cols[14].replace(",", ".").strip() if cols[14] else "0"
    summ = cols[15].replace(",", ".").strip() if cols[15] else "0"
    profit = cols[16].replace(",", ".").strip() if cols[16] else "0"

    # Додаємо до списку
    data.append((
        datatime, type_reg, nom_description, nom_code, nom_type, nom_analssalary, 
        nom_analsale, nazn_description, nazn_code, isp_description, isp_code, 
        posted, quant_vytracheno, quant_sale, price, summ, profit
    ))

# Підключення до MySQL
try:
    conn = pymysql.connect(
        host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_db
    )
    cursor = conn.cursor()

    # # 🛠 Додаємо унікальний ключ (тільки 1 раз, потім цей рядок можна видалити)
    # try:
    #     cursor.execute("ALTER TABLE ent_zp_sale ADD UNIQUE KEY unique_sale (datatime, type_reg, nom_code);")
    #     conn.commit()
    # except pymysql.err.InternalError:
    #     pass  # Якщо ключ вже є, просто продовжуємо

    # SQL-запит для вставки або оновлення
    sql = """
    INSERT INTO ent_zp_sale 
    (datatime, type_reg, nom_description, nom_code, nom_type, nom_analssalary, nom_analsale, 
    nazn_description, nazn_code, isp_description, isp_code, posted, quant_vytracheno, 
    quant_sale, price, summ, profit) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    nom_description=VALUES(nom_description), nom_type=VALUES(nom_type), 
    nom_analssalary=VALUES(nom_analssalary), nom_analsale=VALUES(nom_analsale),
    nazn_description=VALUES(nazn_description), nazn_code=VALUES(nazn_code),
    isp_description=VALUES(isp_description), isp_code=VALUES(isp_code),
    posted=VALUES(posted), quant_vytracheno=VALUES(quant_vytracheno),
    quant_sale=VALUES(quant_sale), price=VALUES(price), summ=VALUES(summ), profit=VALUES(profit);
    """

    cursor.executemany(sql, data)  # Масове завантаження даних
    conn.commit()
    print(f"✅ Успішно вставлено або оновлено {cursor.rowcount} записів.")
    
except pymysql.Error as e:
    print("❌ Помилка MySQL:", e)

finally:
    cursor.close()
    conn.close()
