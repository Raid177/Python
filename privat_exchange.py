import requests
import mysql.connector
from datetime import datetime, timedelta
import time

# Настройки подключения к базе данных
db_config = {
    'user': 'root',
    'password': 'root',
    'host': '127.0.0.1',
    'database': 'test'
}

# Функция для вставки данных в таблицу MariaDB
def insert_exchange_rate(cursor, data):
    sql = """
    INSERT INTO exchange_rates (date, baseCurrency, currency, saleRateNB, purchaseRateNB, saleRate, purchaseRate)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        saleRateNB = VALUES(saleRateNB),
        purchaseRateNB = VALUES(purchaseRateNB),
        saleRate = VALUES(saleRate),
        purchaseRate = VALUES(purchaseRate),
        updated_at = CURRENT_TIMESTAMP
    """
    cursor.execute(sql, data)

# Функция для получения последней даты из таблицы
def get_last_date(cursor):
    cursor.execute("SELECT MAX(date) FROM exchange_rates")
    result = cursor.fetchone()
    return result[0] if result[0] else datetime(2015, 1, 1).date()

# Создание подключения к базе данных
cnx = mysql.connector.connect(**db_config)
cursor = cnx.cursor()

# Получение последней даты из таблицы
last_date = get_last_date(cursor)
start_date = last_date - timedelta(days=1)  # Начальная дата - день после последней даты
end_date = datetime.now().date()

# Перебор дат и отправка запросов
while start_date <= end_date:
    dates = [start_date + timedelta(days=i) for i in range(30) if start_date + timedelta(days=i) <= end_date]
    for date in dates:
        formatted_date = date.strftime('%d.%m.%Y')
        url = f'https://api.privatbank.ua/p24api/exchange_rates?date={formatted_date}'
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            for rate in data['exchangeRate']:
                insert_data = (
                    date.strftime('%Y-%m-%d'),  # Преобразование даты в формат yyyy-mm-dd
                    data['baseCurrencyLit'],
                    rate.get('currency', ''),
                    rate.get('saleRateNB', None),
                    rate.get('purchaseRateNB', None),
                    rate.get('saleRate', None),
                    rate.get('purchaseRate', None)
                )
                print (formatted_date)
                insert_exchange_rate(cursor, insert_data)
            cnx.commit()
        else:
            print(f'Ошибка: {response.status_code} для даты {formatted_date}')

    start_date += timedelta(days=30)
    time.sleep(1)

# Закрытие соединения с базой данных
cursor.close()
cnx.close()
