import requests
from requests.auth import HTTPBasicAuth

# Данные для подключения
odata_url_create = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262-copy/odata/standard.odata/Document_ДенежныйЧек"
odata_username = "odata"  # Замени на свой логин
odata_password = "zX8a7M36yU"  # Замени на свой пароль

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}

# Данные нового документа
data_create = { 
    "Number": "000774407",  # Вставьте свой номер
    "Date": "2025-03-30T00:00:00",
    "Posted": True,
    "Валюта_Key": "da1cdf6a-4e84-11ef-83bb-2ae983d8a0f0",
    "ВидДвижения": "РасчетыПоКредитам",
    "ДенежныйСчет": "f179f3be-4e84-11ef-83bb-2ae983d8a0f0",
    "ДенежныйСчет_Type": "StandardODATA.Catalog_ДенежныеСчета",
    "Комментарий": "Тест",
    "Кратность": "1",
    "Курс": 1,
    "НаправлениеДвижения": "Приход",
    "Объект": "0beebd3e-4ef7-11ef-87da-2ae983d8a0f0",
    "Объект_Type": "StandardODATA.Catalog_Контрагенты",
    "Организация_Key": "e3e20bc4-4e84-11ef-83bb-2ae983d8a0f0",
    "Ответственный_Key": "43996fe4-4e85-11ef-83bb-2ae983d8a0f0",
    "Подразделение_Key": "7f5078ca-4dfe-11ef-978c-2ae983d8a0f0",
    "Сумма": 124947.34,
    "СуммаБезнал": 0
}

# Запрос на создание документа
response_create = requests.post(
    odata_url_create,
    headers=headers,
    auth=HTTPBasicAuth(odata_username, odata_password),
    json=data_create
)

# Проверяем ответ
if response_create.status_code == 201:
    response_data_create = response_create.json()
    ref_key_create = response_data_create.get('Ref_Key')
    print(f"Документ успешно создан! Ref_Key: {ref_key_create}")
else:
    print(f"Ошибка {response_create.status_code}: {response_create.text}")
