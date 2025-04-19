import requests
import pymysql
import xml.etree.ElementTree as ET
import os
import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Завантаження змінних з .env
load_dotenv()

# Параметри БД
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# Параметри OData
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")
ODATA_URL = os.getenv("ODATA_URL") + "AccumulationRegister_ДенежныеСредства/Balance"

# Кількість потоків
MAX_THREADS = 5  # Можна збільшити до 10

# Функція отримання останньої дати з БД
def get_last_balance_date():
    connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
    with connection.cursor() as cursor:
        cursor.execute("SELECT MAX(ДатаВремя) FROM et_AccumulationRegister_ДенежныеСредства_Balance")
        last_date = cursor.fetchone()[0]
    connection.close()
    
    if last_date:
        return last_date - datetime.timedelta(days=21)
    else:
        return datetime.datetime(2024, 7, 30)

# Функція запиту до OData
def fetch_balance_data(date):
    """Запит до OData та парсинг XML у JSON"""
    formatted_date = date.strftime("%Y-%m-%dT00:00:00")
    url = f"{ODATA_URL}(Period=datetime'{formatted_date}')"

    print(f"Отримую дані за {date.strftime('%Y-%m-%d')}")

    try:
        response = requests.get(url, auth=(ODATA_USER, ODATA_PASSWORD), timeout=10)
        response.raise_for_status()  # Викличе помилку, якщо статус-код 4xx або 5xx
        return parse_xml(response.text, date)
    except requests.exceptions.RequestException as e:
        print(f"❌ Помилка запиту {date.strftime('%Y-%m-%d')}: {e}")
        return []

# Функція парсингу XML у список JSON
def parse_xml(xml_data, period_date):
    """Перетворення XML-відповіді у список JSON"""
    try:
        root = ET.fromstring(xml_data)
        namespace = {"d": "http://schemas.microsoft.com/ado/2007/08/dataservices"}
        results = []

        for element in root.findall(".//d:element", namespace):
            data = {
                "ДатаВремя": period_date,  # Використовуємо дату із запиту
                "Организация_Key": element.find("d:Организация_Key", namespace).text,
                "ВидДенежныхСредств": element.find("d:ВидДенежныхСредств", namespace).text,
                "ДенежныйСчет_Key": element.find("d:ДенежныйСчет_Key", namespace).text,
                "СуммаBalance": float(element.find("d:СуммаBalance", namespace).text)  # Перетворюємо в float
            }
            results.append(data)
        
        return results
    except ET.ParseError:
        print(f"❌ Помилка парсингу XML за {period_date.strftime('%Y-%m-%d')}")
        return []

# Функція оновлення БД
def update_database(data):
    """Оновлення БД"""
    if not data:
        return

    connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
    with connection.cursor() as cursor:
        for entry in data:
            cursor.execute("""
                SELECT СуммаBalance FROM et_AccumulationRegister_ДенежныеСредства_Balance 
                WHERE ДатаВремя=%s AND ДенежныйСчет_Key=%s
            """, (entry["ДатаВремя"], entry["ДенежныйСчет_Key"]))
            
            existing = cursor.fetchone()
            
            if existing:
                if float(existing[0]) != entry["СуммаBalance"]:
                    cursor.execute("""
                        UPDATE et_AccumulationRegister_ДенежныеСредства_Balance 
                        SET СуммаBalance=%s, updated_at=NOW()
                        WHERE ДатаВремя=%s AND ДенежныйСчет_Key=%s
                    """, (entry["СуммаBalance"], entry["ДатаВремя"], entry["ДенежныйСчет_Key"]))
            else:
                cursor.execute("""
                    INSERT INTO et_AccumulationRegister_ДенежныеСредства_Balance 
                    (ДатаВремя, Организация_Key, ВидДенежныхСредств, ДенежныйСчет_Key, СуммаBalance, created_at, updated_at) 
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                """, (entry["ДатаВремя"], entry["Организация_Key"], entry["ВидДенежныхСредств"], entry["ДенежныйСчет_Key"], entry["СуммаBalance"]))
    
    connection.commit()
    connection.close()

# Функція обробки дати в потоці
def process_date(date):
    """Обробка конкретного дня (отримання + запис у БД)"""
    data = fetch_balance_data(date)
    if data:
        update_database(data)

# Основний процес (з багатопотоковістю)
def main():
    """Головна функція"""
    start_date = get_last_balance_date()
    end_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    print(f"🚀 Отримуємо дані з {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}, потоки: {MAX_THREADS}")

    dates = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    # Запускаємо потоки
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_date = {executor.submit(process_date, date): date for date in dates}
        
        for future in as_completed(future_to_date):
            date = future_to_date[future]
            try:
                future.result()  # Отримуємо результат виконання
            except Exception as e:
                print(f"❌ Помилка обробки {date.strftime('%Y-%m-%d')}: {e}")

if __name__ == "__main__":
    main()
