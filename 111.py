def fetch_data():
    offset = 0
    limit = 56
    total_records = 0

    connection = connect_to_mysql()
    cursor = connection.cursor()

    # Дата початку фільтрації
    filter_date = "2024-11-01T00:00:00"

    while True:
        params = {
            "$top": limit,
            "$skip": offset,
            "$filter": f"Date ge datetime'{filter_date}'",  # Фільтр по даті
            "$format": "json"
        }

        print(f"Виконую запит до API з параметрами: {params}")  # Додано діагностику

        response = requests.get(api_url, auth=api_auth, params=params)

        # Додано перевірку статусу відповіді
        if response.status_code != 200:
            print(f"Помилка при отриманні даних: {response.status_code} {response.text}")
            break

        data = response.json().get("value", [])
        print(f"Отримано {len(data)} записів від API")  # Додано вивід кількості записів

        if not data:
            print("Завантаження завершено.")
            break

        # Формування списку записів для вставки
        records = []
        for item in data:
            base_record = {
                "Ref_Key": item.get("Ref_Key"),
                "DataVersion": item.get("DataVersion"),
                "DeletionMark": item.get("DeletionMark"),
                "Number": item.get("Number"),
                "Date": item.get("Date"),
                "Posted": item.get("Posted"),
                "Организация_Key": item.get("Организация_Key"),
                "Ответственный_Key": item.get("Ответственный_Key"),
                "СуммаДокумента": item.get("СуммаДокумента"),
                "Комментарий": item.get("Комментарий"),
            }

            for employee in item.get("Сотрудники", []):
                record = base_record.copy()
                record.update({
                    "LineNumber": employee.get("LineNumber"),
                    "Сотрудник_Key": employee.get("Сотрудник_Key"),
                    "Сумма": employee.get("Сумма"),
                    "Статья_Key": employee.get("Статья_Key"),
                    "Комментарий_Статья": employee.get("Комментарий"),
                })
                records.append(record)

        try:
            if records:
                insert_records(records, cursor)
                connection.commit()
                total_records += len(records)
                print(f"Збережено {len(records)} записів у базу, всього: {total_records}")
            else:
                print("Немає записів для збереження.")
        except mysql.connector.Error as e:
            print(f"Помилка при роботі з MySQL: {e}")
            connection.rollback()

        offset += limit
        time.sleep(3)

    cursor.close()
    connection.close()
