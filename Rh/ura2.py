import requests
from requests.auth import HTTPBasicAuth
import json

ref_key = "c316e9c8-3a65-11f0-8de8-2ae983d8a0f0"
base_url = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262-copy/odata/standard.odata"
url = f"{base_url}/Document_Анализы(guid'{ref_key}')"
auth = HTTPBasicAuth("odata", "zX8a7M36yU")
headers = {"Content-Type": "application/json", "Accept": "application/json"}

# GET
r = requests.get(url, auth=auth, headers=headers)
if r.status_code != 200:
    print("GET помилка:", r.status_code, r.text)
    exit()

data = r.json()

# 🧪 Тестово змінити 2 ОткрытыйОтвет у відповідних LineNumber
for row in data.get("Состав", []):
    if row.get("LineNumber") == "1":
        row["ОткрытыйОтвет"] = "🔁 Відповідь 1 - тестова зміна"
    elif row.get("LineNumber") == "2":
        row["ОткрытыйОтвет"] = "🔁 Відповідь 2 - Якість зображення достатня для аналізу"

# PUT
put = requests.put(url, auth=auth, headers=headers, json=data)
print("PUT статус:", put.status_code)
print("Відповідь:", put.text)