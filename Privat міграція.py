import pymysql
import os
import re
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
            # Отримуємо найновішу дату DAT_OD у bnk_trazact_prvt_ekv
            cursor.execute("SELECT MAX(DAT_OD) AS max_date FROM bnk_trazact_prvt_ekv")
            result = cursor.fetchone()
            max_date = result["max_date"]
            
            # Якщо дат немає, беремо 1 липня 2024
            if not max_date:
                max_date = datetime(2024, 7, 1)
            else:
                max_date -= timedelta(days=1)
            
            print(f"Вибірка записів від дати: {max_date}")
            cursor.execute("SELECT COUNT(*) AS count FROM bnk_trazact_prvt WHERE DAT_OD >= %s", (max_date,))
            count_result = cursor.fetchone()
            print(f"Кількість записів для перенесення: {count_result['count']}")
            
            cursor.execute("SELECT * FROM bnk_trazact_prvt WHERE DAT_OD >= %s", (max_date,))
            rows = cursor.fetchall()
            
            if not rows:
                print("Немає нових даних для переносу.")
                return
            
            for row in rows:
                new_rows = [row.copy()]
                
                # Перевіряємо умови
                if row['AUT_CNTR_NAM'] == "Розрахунки з еквайрингу" and row['OSND'].startswith("cmps: 12"):
                    modified_row = row.copy()
                    modified_row['NUM_DOC'] += "_ek"
                    modified_row['TRANTYPE'] = "D" if row['TRANTYPE'] == "C" else row['TRANTYPE']
                    modified_row['OSND'] = "Комісія за екваринг"
                    commission_amount = extract_commission(row['OSND'])
                    modified_row['SUM'] = commission_amount
                    modified_row['SUM_E'] = commission_amount
                    new_rows.append(modified_row)
                
                # Записуємо дані у bnk_trazact_prvt_ekv з обробкою дублікатів
                for new_row in new_rows:
                    placeholders = ", ".join(["%s"] * len(new_row))
                    columns = ", ".join(new_row.keys())
                    updates = ", ".join([f"{col} = VALUES({col})" for col in new_row.keys() if col not in ('NUM_DOC', 'DATE_TIME_DAT_OD_TIM_P', 'AUT_CNTR_MFO', 'TRANTYPE')])
                    sql = f"INSERT INTO bnk_trazact_prvt_ekv ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
                    cursor.execute(sql, tuple(new_row.values()))
            
            connection.commit()
            print(f"Перенесено {len(rows)} записів.")
    finally:
        connection.close()

if __name__ == "__main__":
    migrate_data()
    print("Міграція завершена!")