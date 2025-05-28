import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

ref_key = "c316e9c8-3a65-11f0-8de8-2ae983d8a0f0"
base_url = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262-copy/odata/standard.odata"
url_get = f"{base_url}/Document_Анализы(guid'{ref_key}')"
url_put = url_get

auth = HTTPBasicAuth("odata", "zX8a7M36yU")
headers = {"Content-Type": "application/json", "Accept": "application/json"}

# GET
response = requests.get(url_get, auth=auth, headers=headers)
if response.status_code != 200:
    print("GET помилка:", response.status_code, response.text)
    exit()

data = response.json()

# Заміна поля `Ответ` у першому елементі Состав
data["Состав"][0]["Ответ"] = "Пarsgsfaferwqaактерні для хронічного запального процесу"

# PUT
put_response = requests.put(url_put, auth=auth, headers=headers, json=data)

print("PUT статус:", put_response.status_code)
print("Відповідь:", put_response.text)
