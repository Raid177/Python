import requests
from requests.auth import HTTPBasicAuth

# Данные для подключения
odata_url_create = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262-copy/odata/standard.odata/Catalog_Контрагенты"
odata_username = "odata"  # Замени на свой логин
odata_password = "zX8a7M36yU"  # Замени на свой пароль

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}

# Данные нового контрагента
data_create = {
    "DeletionMark": False,
    "Parent_Key": "00000000-0000-0000-0000-000000000000",
    "IsFolder": False,
    "Description": "MyTest3",
    "ТипЦен_Key": "a6db56f8-4ef8-11ef-87da-2ae983d8a0f0",
    "ВалютаВзаиморасчетов_Key": "da1cdf6a-4e84-11ef-83bb-2ae983d8a0f0"
}

# Запрос на создание контрагента
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
    print(f"Контрагент успешно создан! Ref_Key: {ref_key_create}")
else:
    print(f"Ошибка {response_create.status_code}: {response_create.text}")
