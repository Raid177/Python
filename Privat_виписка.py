import requests
from dotenv import load_dotenv
import os
import csv

# Завантажуємо змінні з .env файлу
load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')  # Беремо токен із .env файлу
url = 'https://acp.privatbank.ua/api/statements/transactions'
headers = {
    'User-Agent': 'PythonClient',  # Ім'я вашого клієнта
    'token': API_TOKEN,  # Ваш токен
    'Content-Type': 'application/json;charset=cp1251'  # Важливо вказати cp1251
}

params = {
    'acc': 'UA973052990000026002025035545',  # Ваш рахунок
    'startDate': '01-02-2025',
    'endDate': '13-02-2025',
    'limit': '100'  # Максимальна кількість записів на запит
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    # Виводимо JSON-дані
    data = response.json()

    # Перевірка на успішність відповіді
    if data.get('status') == 'SUCCESS':
        transactions = data.get('transactions', [])

        # Динамічно отримуємо всі унікальні ключі з транзакцій
        all_keys = set()
        for transaction in transactions:
            all_keys.update(transaction.keys())

        # Записуємо в CSV файл
        with open('transactions.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=all_keys)
            writer.writeheader()  # Записуємо заголовки

            # Записуємо кожен запис транзакції
            for transaction in transactions:
                writer.writerow(transaction)
                
        print("Дані успішно збережено в файл transactions.csv")
    else:
        print("Помилка в отриманні транзакцій:", data.get('message'))
else:
    print(f"Помилка {response.status_code}: {response.text}")
