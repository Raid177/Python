import requests

# URL до ендпоінта API
api_url = "https://cabinet.tax.gov.ua/api/zreports"  # Замініть на актуальний ендпоінт

# Токен для авторизації
headers = {
    "Authorization": "Bearer 46c4409d-4d35-4327-b140-768de50f63f6",  # Ваш токен
    "Content-Type": "application/json",
    "User-Agent": "PythonClient/1.0"  # Додатковий заголовок
}

# Параметри запиту (замініть на потрібні)
params = {
    "startDate": "2024-01-01",  # Початкова дата
    "endDate": "2024-01-31"     # Кінцева дата
}

# Виконання запиту
try:
    response = requests.get(api_url, headers=headers, params=params)

    # Перевірка статусу відповіді
    if response.status_code == 200:
        try:
            # Парсинг JSON
            data = response.json()
            print("Дані успішно отримані:")
            print(data)

            # Збереження результатів у файл
            with open("zreports.json", "w", encoding="utf-8") as file:
                import json
                json.dump(data, file, ensure_ascii=False, indent=4)
        except requests.exceptions.JSONDecodeError:
            print("Сервер повернув не JSON:")
            print(response.text)  # Виведення тексту відповіді
            with open("response.log", "w", encoding="utf-8") as log_file:
                log_file.write(response.text)
    else:
        print(f"Помилка: {response.status_code}")
        print(f"Відповідь сервера: {response.text}")
        with open("error.log", "w", encoding="utf-8") as error_file:
            error_file.write(f"Код помилки: {response.status_code}\n")
            error_file.write(response.text)

except requests.RequestException as e:
    print(f"Помилка запиту: {e}")
