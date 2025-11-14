#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL: OData Catalog_Контрагенты → MySQL petwealth.et_x_Catalog_Контрагенты
Авторизація з /root/Python/.env або з ENV_FILE
UPSERT по Ref_Key з оновленням DataVersion, Description, ЕДРПОУ, IBAN1, IBAN2
"""

import os
import json
import time
import requests
import mysql.connector
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1️⃣ Завантаження .env (Hetzner)
# ---------------------------------------------------------
ENV_FILE = os.getenv("ENV_FILE", "/root/Python/.env")
load_dotenv(dotenv_path=ENV_FILE)

# ---------------------------------------------------------
# 2️⃣ Параметри з .env
# ---------------------------------------------------------
ODATA_URL_BASE = os.getenv("ODATA_URL", "").rstrip("/") + "/"
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE", "petwealth")

TABLE = "et_x_Catalog_Контрагенты"

# ---------------------------------------------------------
# 3️⃣ Перевірка параметрів
# ---------------------------------------------------------
if not all([ODATA_URL_BASE, ODATA_USER, ODATA_PASSWORD]):
    raise RuntimeError("❌ Не задані ODATA_URL / ODATA_USER / ODATA_PASSWORD у .env")

ODATA_URL = f"{ODATA_URL_BASE}Catalog_Контрагенты?$format=json"

# ---------------------------------------------------------
# 4️⃣ Підключення до БД
# ---------------------------------------------------------
conn = mysql.connector.connect(
    host=DB_HOST, port=DB_PORT,
    user=DB_USER, password=DB_PASSWORD,
    database=DB_DATABASE,
    charset='utf8mb4', use_unicode=True
)
cursor = conn.cursor()

# ---------------------------------------------------------
# 5️⃣ Допоміжна функція для пагінації OData
# ---------------------------------------------------------
def fetch_all(session, base_url, auth, top=1000, delay=0.5):
    results, skip = [], 0
    while True:
        url = f"{base_url}&$top={top}&$skip={skip}"
        r = session.get(url, auth=auth)
        r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            break
        results.extend(batch)
        skip += top
        if delay:
            time.sleep(delay)
    return results

# ---------------------------------------------------------
# 6️⃣ Основна логіка
# ---------------------------------------------------------
session = requests.Session()
added_count = 0
updated_count = 0

entries = fetch_all(session, ODATA_URL, (ODATA_USER, ODATA_PASSWORD), top=1000, delay=0.0)

for entry in entries:
    sklad = {}
    for q in entry.get('Состав', []) or []:
        if isinstance(q, dict):
            sklad[q.get('Вопрос_Key')] = q.get('Ответ')

    edrpou = sklad.get('c53d792c-4ef4-11ef-87da-2ae983d8a0f0')
    iban1  = sklad.get('f61f85c6-4ef4-11ef-87da-2ae983d8a0f0')
    iban2  = sklad.get('42667dea-4ef5-11ef-87da-2ae983d8a0f0')

    # Підготовка полів
    contact_info = entry.get('КонтактнаяИнформация')
    if contact_info is not None and not isinstance(contact_info, str):
        try:
            contact_info = json.dumps(contact_info, ensure_ascii=False)
        except Exception:
            contact_info = str(contact_info)

    ref_key = entry.get('Ref_Key')
    dataversion = entry.get('DataVersion')

    cursor.execute(f"SELECT DataVersion FROM `{TABLE}` WHERE Ref_Key = %s", (ref_key,))
    existing = cursor.fetchone()

    cursor.execute(f"""
        INSERT INTO `{TABLE}`
        (Ref_Key, DataVersion, DeletionMark, Parent_Key, IsFolder, Code, Description,
         ТипЦен_Key, ВалютаВзаиморасчетов_Key, КонтактнаяИнформация, Комментарий,
         ОтсрочкаПлатежа, КодВнешнейБазы, Менеджер_Key, ПремияПолучена, АнкетаЗаполнена,
         ЭтоВнешняяЛаборатория, ЭтоПоставщик, ЭтоРеферент, ИНН, ЕДРПОУ, IBAN1, IBAN2)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            DataVersion = VALUES(DataVersion),
            Description = VALUES(Description),
            ЕДРПОУ      = VALUES(ЕДРПОУ),
            IBAN1       = VALUES(IBAN1),
            IBAN2       = VALUES(IBAN2)
    """, (
        entry.get('Ref_Key'), dataversion, entry.get('DeletionMark'), entry.get('Parent_Key'),
        entry.get('IsFolder'), entry.get('Code'), entry.get('Description'),
        entry.get('ТипЦен_Key'), entry.get('ВалютаВзаиморасчетов_Key'),
        contact_info, entry.get('Комментарий'),
        entry.get('ОтсрочкаПлатежа'), entry.get('КодВнешнейБазы'),
        entry.get('Менеджер_Key'), entry.get('ПремияПолучена'), entry.get('АнкетаЗаполнена'),
        entry.get('ЭтоВнешняяЛаборатория'), entry.get('ЭтоПоставщик'), entry.get('ЭтоРеферент'),
        entry.get('ИНН'), edrpou, iban1, iban2
    ))

    if cursor.rowcount > 0:
        if existing is None:
            added_count += 1
        else:
            updated_count += 1

# ---------------------------------------------------------
# 7️⃣ Завершення
# ---------------------------------------------------------
conn.commit()
cursor.close()
conn.close()

print(f"✅ Завантаження Catalog_Контрагенты виконано успішно → {TABLE}")
print(f"📌 Додано нових записів: {added_count}")
print(f"🔄 Оновлено записів: {updated_count}")
print(f"🔧 Використано ENV: {ENV_FILE}")
