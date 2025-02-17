import pymysql
import os
import re
from dotenv import load_dotenv

# Завантажуємо змінні середовища
load_dotenv()

# Дані для підключення до БД
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "cursorclass": pymysql.cursors.DictCursor
}

def extract_commission(osnd_text):
    """Витягуємо суму комісії з тексту OSND."""
    match = re.search(r"Ком бан ([\d.]+)грн", osnd_text)
    return float(match.group(1)) if match else 0.0

def migrate_data():
    """Переносимо дані між таблицями."""
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            # Отримуємо всі записи з bnk_trazact_prvt
            cursor.execute("SELECT * FROM bnk_trazact_prvt")
            rows = cursor.fetchall()
            
            for row in rows:
                new_rows = [row.copy()]
                
                # Перевіряємо умови
                if row['AUT_CNTR_NAM'] == "Розрахунки з еквайрингу" and row['OSND'].startswith("cmps: 12"):
                    modified_row = row.copy()
                    modified_row['NUM_DOC'] += "_ek"
                    modified_row['TRANTYPE'] = "D" if row['TRANTYPE'] == "C" else row['TRANTYPE']
                    modified_row['OSND'] = "Комісія банку за еквайринг"
                    modified_row['SUM'] = extract_commission(row['OSND'])
                    new_rows.append(modified_row)
                
                # Записуємо дані у bnk_trazact_prvt_ekv
                for new_row in new_rows:
                    placeholders = ", ".join(["%s"] * len(new_row))
                    columns = ", ".join(new_row.keys())
                    sql = f"INSERT INTO bnk_trazact_prvt_ekv ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, tuple(new_row.values()))
            
            connection.commit()
    finally:
        connection.close()

if __name__ == "__main__":
    migrate_data()
    print("Міграція завершена!")
