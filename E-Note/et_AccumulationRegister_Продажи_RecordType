# ============================================================
# Скрипт: et_AccumulationRegister_Продажи_RecordType
# ------------------------------------------------------------
# Призначення:
#   Автоматично синхронізує дані з OData (AccumulationRegister_Продажи_RecordType)
#   у таблицю MySQL (et_AccumulationRegister_Продажи_RecordType),
#   враховуючи додані, змінені та видалені записи.
#
# Основні етапи:
#   1️⃣ Визначає останню дату Period у БД і відмотує на DAYS_BACK днів назад.
#   2️⃣ Отримує з OData усі записи, де Period >= цієї дати.
#   3️⃣ Завантажує отримані дані у тимчасову таблицю (стейджинг) stg_Продажи.
#   4️⃣ Виконує MERGE у три кроки:
#        • INSERT — додає нові записи;
#        • UPDATE — оновлює існуючі записи у вікні;
#        • DELETE — видаляє ті, яких більше немає в Єноті.
#   5️⃣ Зберігає created_at для старих рядків і оновлює updated_at.
#   6️⃣ Працює батчами по BATCH_SIZE із паузою SLEEP_SECONDS між запитами.
#
# Переваги:
#   ✅ Коректно обробляє видалення з боку Єнота.
#   ✅ Не створює “дірки” у даних (оновлення без повного wipe).
#   ✅ Мінімальне навантаження на БД — змінює лише необхідне.
#   ✅ Зберігає історію створення / оновлення записів.
#
# Налаштування (.env):
#   ODATA_URL, ODATA_USER, ODATA_PASSWORD,
#   DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
#
# Опційні змінні:
#   DAYS_BACK, BATCH_SIZE, SLEEP_SECONDS
#
# Вимоги до БД:
#   PRIMARY KEY (Recorder, LineNumber)
#   INDEX (Period)
#
# ------------------------------------------------------------
# Автор: ChatGPT (GPT-5), спеціально для Олексія Льодіна :)
# Дата: 2025-10-13
# ============================================================


import os
import time
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ===================== CONFIG =====================
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")
ODATA_URL = os.getenv("ODATA_URL")

# Параметри вікна та завантаження
DAYS_BACK = int(os.getenv("DAYS_BACK", "25"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1.0"))

# Якщо треба форсити стартову дату при пустій таблиці
FALLBACK_START = datetime(2024, 7, 20)

TARGET_TABLE = "et_AccumulationRegister_Продажи_RecordType"
STAGE_TABLE  = "stg_Продажи"  # TIMP TABLE
PK_FIELDS = ("Recorder", "LineNumber")

FIELDS = [
    "Recorder", "LineNumber", "Period", "Recorder_Type", "Active",
    "Номенклатура_Key", "Организация_Key", "Подразделение_Key",
    "Контрагент_Key", "Карточка_Key", "Сотрудник", "Сотрудник_Type",
    "Исполнитель", "Исполнитель_Type", "Склад_Key",
    "ОрганизацияИсполнителя_Key", "ПодразделениеИсполнителя_Key",
    "Количество", "КоличествоОплачено", "Стоимость",
    "СтоимостьБезСкидок", "СуммаЗатрат"
]
# ==================================================

def get_conn():
    return mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE
    )

def get_last_period():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(Period) FROM {TARGET_TABLE}")
    last_period = cur.fetchone()[0]
    cur.close(); conn.close()
    if last_period:
        return last_period - timedelta(days=DAYS_BACK)
    return FALLBACK_START

def build_base_odata_url(start_iso):
    if not ODATA_URL:
        raise ValueError("ODATA_URL не знайдено в змінних середовища")
    # Можеш вказати $select=... якщо хочеш обмежити поля (імена мають відповідати OData)
    return (
        f"{ODATA_URL}AccumulationRegister_Продажи_RecordType?$format=json"
        f"&$filter=Period ge datetime'{start_iso}'"
        f"&$orderby=Period"
    )

def create_stage_table(conn):
    cur = conn.cursor()
    # створюємо TEMPORARY таблицю як у таргеті
    cur.execute(f"CREATE TEMPORARY TABLE {STAGE_TABLE} LIKE {TARGET_TABLE}")
    # послабимо timestamps у стейджі, щоб можна не заповнювати
    try:
        cur.execute(f"ALTER TABLE {STAGE_TABLE} MODIFY created_at DATETIME NULL")
    except:
        pass
    try:
        cur.execute(f"ALTER TABLE {STAGE_TABLE} MODIFY updated_at DATETIME NULL")
    except:
        pass
    conn.commit()
    cur.close()

def fetch_into_stage(conn, base_url):
    session = requests.Session()
    session.auth = (ODATA_USER, ODATA_PASSWORD)

    cur = conn.cursor()
    inserted_rows = 0
    skip = 0

    insert_sql = f"""
        INSERT INTO {STAGE_TABLE} (
            {", ".join(FIELDS)}
        ) VALUES (
            {", ".join(["%({})s".format(f) for f in FIELDS])}
        )
    """

    while True:
        next_url = f"{base_url}&$top={BATCH_SIZE}&$skip={skip}"
        r = session.get(next_url, timeout=60)
        if r.status_code != 200:
            print(f"[ERROR] OData {r.status_code}: {r.text}")
            break

        batch = r.json().get("value", [])
        if not batch:
            if skip == 0:
                print("[INFO] Дані відсутні за обраним вікном.")
            else:
                print("[INFO] Отримано всі записи з OData.")
            break

        # невеликий лог
        try:
            min_p = min(x["Period"] for x in batch)
            max_p = max(x["Period"] for x in batch)
        except Exception:
            min_p = max_p = "N/A"

        print(f"[FETCH] {len(batch)} записів (skip={skip}) | {min_p} .. {max_p}")

        # пакетна вставка у стейдж
        cur.executemany(insert_sql, batch)
        conn.commit()
        inserted_rows += cur.rowcount

        skip += len(batch)
        time.sleep(SLEEP_SECONDS)

    cur.close()
    print(f"[STAGE] Вставлено у стейдж: {inserted_rows} рядків")

def merge_stage_into_target(conn, cutoff_dt):
    cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.cursor()

    # 3a) INSERT нових (БЕЗ alias для цільової таблиці!)
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            {", ".join(FIELDS)}, created_at, updated_at
        )
        SELECT
            {", ".join("s."+f for f in FIELDS)}, NOW(), NOW()
        FROM {STAGE_TABLE} s
        LEFT JOIN {TARGET_TABLE} t
          ON {" AND ".join([f"t.{pk}=s.{pk}" for pk in PK_FIELDS])}
        WHERE t.{PK_FIELDS[0]} IS NULL
    """

    # 3b) UPDATE наявних (обмежуємося вікном по Period)
    set_list = [
        f"t.{f} = s.{f}" for f in FIELDS if f not in PK_FIELDS
    ]
    update_sql = f"""
        UPDATE {TARGET_TABLE} t
        JOIN {STAGE_TABLE} s
          ON {" AND ".join([f"t.{pk}=s.{pk}" for pk in PK_FIELDS])}
        SET {", ".join(set_list)},
            t.updated_at = NOW()
        WHERE t.Period >= %s
    """

    # 3c) DELETE відсутніх у стейджі
    delete_sql = f"""
        DELETE t
        FROM {TARGET_TABLE} t
        LEFT JOIN {STAGE_TABLE} s
          ON {" AND ".join([f"t.{pk}=s.{pk}" for pk in PK_FIELDS])}
        WHERE t.Period >= %s
          AND s.{PK_FIELDS[0]} IS NULL
    """

    conn.start_transaction()

    cur.execute(insert_sql)
    inserted = cur.rowcount

    cur.execute(update_sql, (cutoff_str,))
    updated = cur.rowcount

    cur.execute(delete_sql, (cutoff_str,))
    deleted = cur.rowcount

    conn.commit()
    cur.close()

    print(f"[MERGE] inserted={inserted}, updated={updated}, deleted={deleted}")

def main():
    cutoff = get_last_period()
    start_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%S")
    base_url = build_base_odata_url(start_iso)

    print(f"[INFO] Вікно: Period >= {start_iso} (DAYS_BACK={DAYS_BACK})")
    print(f"[INFO] BATCH_SIZE={BATCH_SIZE}, SLEEP_SECONDS={SLEEP_SECONDS}")

    conn = get_conn()
    try:
        create_stage_table(conn)
        fetch_into_stage(conn, base_url)
        merge_stage_into_target(conn, cutoff)
        print("[DONE] Синхронізація завершена.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
