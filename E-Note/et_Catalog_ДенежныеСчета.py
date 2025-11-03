import os
import sys
import time
import json
import requests
import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

# ---------------------------
# 1) .env з фіксованого шляху
# ---------------------------
ENV_PATH = Path("/root/Python/_Acces/.env.prod")
if not ENV_PATH.exists():
    print(f"[ERROR] .env файл не знайдено: {ENV_PATH}")
    sys.exit(1)

load_dotenv(dotenv_path=ENV_PATH)

# ---------------------------
# 2) Змінні середовища
# ---------------------------
ODATA_BASE = os.getenv("ODATA_URL")  # має закінчуватись на .../odata/standard.odata/
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# Валідація критичних змінних
for key, val in {
    "ODATA_URL": ODATA_BASE,
    "ODATA_USER": ODATA_USER,
    "ODATA_PASSWORD": ODATA_PASSWORD,
    "DB_HOST": DB_HOST,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD,
    "DB_DATABASE": DB_DATABASE,
}.items():
    if not val:
        print(f"[ERROR] Не задано змінну середовища: {key}")
        sys.exit(1)

# ---------------------------
# 3) HTTP-сесія з ретраями
# ---------------------------
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.7, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))
session.auth = (ODATA_USER, ODATA_PASSWORD)
TIMEOUT = 30

# ---------------------------
# 4) Параметри OData
# ---------------------------
# Тягнемо лише потрібні поля
SELECT_FIELDS = [
    "Ref_Key",
    "DataVersion",
    "DeletionMark",
    "Parent_Key",
    "IsFolder",
    "Code",
    "Description",
    "ВидСчета",
    "НомерСчета",
    "Банк_Key",
]

SELECT = ",".join(SELECT_FIELDS)

PAGE_SIZE = 1000
CATALOG_URL = (
    f"{ODATA_BASE}Catalog_ДенежныеСчета"
    f"?$format=json&$select={SELECT}&$orderby=Ref_Key&$top={PAGE_SIZE}"
)

# ---------------------------
# 5) Підключення до MySQL
# ---------------------------
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE,
    autocommit=False,
    charset="utf8mb4",
    use_unicode=True,
)
cursor = conn.cursor()

def preload_versions() -> dict:
    """Завантажуємо Ref_Key -> DataVersion для скорочення кількості UPDATE."""
    print("[INFO] Попереднє завантаження DataVersion з БД ...")
    cursor.execute("SELECT Ref_Key, DataVersion FROM et_Catalog_ДенежныеСчета")
    d = {ref: dv for ref, dv in cursor.fetchall()}
    print(f"[INFO] Завантажено версій із БД: {len(d)}")
    return d

EXISTING = preload_versions()

# ---------------------------
# 6) Головний цикл завантаження
# ---------------------------
total_fetched = 0
total_insert = 0
total_update = 0
total_skip = 0
skip = 0

print("[INFO] Старт завантаження з OData Catalog_ДенежныеСчета ...")

while True:
    url = CATALOG_URL + (f"&$skip={skip}" if skip else "")
    resp = session.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    entries = payload.get("value", [])
    batch_count = len(entries)

    if batch_count == 0:
        break

    total_fetched += batch_count

    ins = 0
    upd = 0
    skp = 0

    for entry in entries:
        ref = entry.get("Ref_Key")
        dv = entry.get("DataVersion")

        # Пропускаємо, якщо версія не змінилась
        if ref in EXISTING and EXISTING[ref] == dv:
            skp += 1
            continue

        # Upsert тільки якщо новий або змінився DataVersion
        cursor.execute(
            """
            INSERT INTO et_Catalog_ДенежныеСчета
            (
                Ref_Key, DataVersion, DeletionMark, Parent_Key, IsFolder,
                Code, Description, ВидСчета, НомерСчета, Банк_Key
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                DataVersion = VALUES(DataVersion),
                DeletionMark = VALUES(DeletionMark),
                Parent_Key = VALUES(Parent_Key),
                IsFolder = VALUES(IsFolder),
                Code = VALUES(Code),
                Description = VALUES(Description),
                ВидСчета = VALUES(ВидСчета),
                НомерСчета = VALUES(НомерСчета),
                Банк_Key = VALUES(Банк_Key)
            """,
            (
                entry.get("Ref_Key"),
                entry.get("DataVersion"),
                entry.get("DeletionMark"),
                entry.get("Parent_Key"),
                entry.get("IsFolder"),
                entry.get("Code"),
                entry.get("Description"),
                entry.get("ВидСчета"),
                entry.get("НомерСчета"),
                entry.get("Банк_Key"),
            ),
        )

        if ref in EXISTING:
            upd += 1
        else:
            ins += 1

        # Оновлюємо локальний кеш версій
        EXISTING[ref] = dv

    conn.commit()

    total_insert += ins
    total_update += upd
    total_skip += skp

    print(
        f"[BATCH] fetched={batch_count} | insert={ins} update={upd} skip(no-change)={skp} | $skip={skip}"
    )

    if batch_count < PAGE_SIZE:
        break

    skip += PAGE_SIZE
    # Якщо API суворе — невелика пауза
    time.sleep(0.5)

cursor.close()
conn.close()

print(
    f"✅ Готово: fetched={total_fetched}, inserted={total_insert}, updated={total_update}, skipped={total_skip}"
)
