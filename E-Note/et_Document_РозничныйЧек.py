#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL: OData Document_РозничныйЧек -> MySQL et_Document_РозничныйЧек

Авторизація з .env:
  ODATA_URL, ODATA_USER, ODATA_PASSWORD
  DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

Режими:
  LOAD_MODE = "rewrite_range"         — видаляємо з БД все з Date >= START_DATE і вставляємо заново (безпечний проти видалених у джерелі)
  LOAD_MODE = "upsert_by_dataversion" — не видаляємо; оновлюємо, якщо DataVersion змінився, інакше пропускаємо
"""

import os
import time
import datetime as dt
from decimal import Decimal
import requests
import pymysql
from dotenv import load_dotenv

# ====================== ПАРАМЕТРИ ======================
LOAD_MODE = "rewrite_range"   # "rewrite_range" або "upsert_by_dataversion"
BATCH_SIZE = 1000             # розмір порції OData
SLEEP_SECONDS = 1.0           # пауза між порціями
DAYS_BACK = 45                # мінус від MAX(Date) у БД
START_IF_EMPTY = "2024-07-01" # якщо таблиця порожня, стартова дата

TABLE  = "et_Document_РозничныйЧек"
ENTITY = "Document_РозничныйЧек"

# Явно фіксуємо всі поля, що тягнемо з OData (має збігатися з типами нижче)
SELECT_FIELDS = [
    "Ref_Key","DataVersion","DeletionMark","Number","Date","Posted",
    "SOVA_UDSdiscountRate","SOVA_UDSIDКлиента","SOVA_UDSIDОперации","SOVA_UDSUIDКлиента","SOVA_UDSUParticipantIDКлиента",
    "SOVA_UDSВсяСуммаБезСкидки","SOVA_UDSИмяКлиента","SOVA_UDSИспользоватьДополнительныйБонус","SOVA_UDSКассир","SOVA_UDSКассир_Type",
    "SOVA_UDSКодСкидки","SOVA_UDSНакопленоБаллов","SOVA_UDSОперацияЗарегистрированаНаСервере","SOVA_UDSПолныйОтветСервераВРезультатеОплаты",
    "SOVA_UDSРассчитанныйПроцентСкидки","SOVA_UDSРасчетДополнительногоБонуса","SOVA_UDSСписываемыеБаллы","SOVA_UDSСуммаБезСкидки",
    "SOVA_UDSСуммаДополнительногоНачисления",
    "ВидОперации","ДенежныйСчет_Key","ДенежныйСчетБезнал_Key","ДенежныйСчетКредит_Key","ДисконтнаяКарточка_Key",
    "КассирИНН","КассирФИО","КассоваяСмена_Key","КодАвторизации","Комментарий","КорректировкаПоЛечению","МДЛПИД","НеПередаватьКассира",
    "НомерПлатежнойКарты","НомерЧекаЭТ","ОкруглятьИтогЧека","Организация_Key","Основание_Key","Ответственный_Key","ОтправлятьEmail",
    "ОтправлятьСМС","Подразделение_Key","Сдача","СистемаНалогообложения","Состояние","СпособОкругленияИтогаЧека","СсылочныйНомер",
    "СуммаДокумента","СуммаОплатыБезнал","СуммаОплатыБонусами","СуммаОплатыКредитом","СуммаОплатыНал","СуммаТорговойУступки",
    "ТипЦен_Key","УказанныйEmail","УказанныйТелефон","ФискальныйНомерЧека","Электронно",
    "MistyLoyalty_ПараметрыОперации_Type","MistyLoyalty_ПараметрыОперации_Base64Data",
    "ЭквайрИД","ТерминалИД","ПлатежнаяСистемаЭТ","СсылочныйНомерОснования","MistyLoyaltyOperationID","СуммаВключаетНДС","ДокументБезНДС",
    "ЭквайрНаименование","ДругоеСредствоОплаты_Key","Контрагент_Key","Карточка_Key","ДенежныйСчетБезналДСО_Key","СуммаБезналДСО",
    "ЧасоваяЗона","Проверен"
]

# Типи колонок у БД (для автодобудови схеми)
COLUMN_TYPES = {
    "Ref_Key": "CHAR(36)",
    "DataVersion": "VARCHAR(50)",  # запас під base64/довжину
    "DeletionMark": "TINYINT(1)",
    "Number": "VARCHAR(20)",
    "Date": "DATETIME",
    "Posted": "TINYINT(1)",

    "SOVA_UDSdiscountRate": "DECIMAL(18,4)",
    "SOVA_UDSIDКлиента": "VARCHAR(100)",
    "SOVA_UDSIDОперации": "VARCHAR(100)",
    "SOVA_UDSUIDКлиента": "VARCHAR(100)",
    "SOVA_UDSUParticipantIDКлиента": "VARCHAR(100)",
    "SOVA_UDSВсяСуммаБезСкидки": "DECIMAL(18,2)",
    "SOVA_UDSИмяКлиента": "VARCHAR(255)",
    "SOVA_UDSИспользоватьДополнительныйБонус": "TINYINT(1)",
    "SOVA_UDSКассир": "VARCHAR(255)",
    "SOVA_UDSКассир_Type": "VARCHAR(100)",
    "SOVA_UDSКодСкидки": "VARCHAR(100)",
    "SOVA_UDSНакопленоБаллов": "DECIMAL(18,2)",
    "SOVA_UDSОперацияЗарегистрированаНаСервере": "TINYINT(1)",
    "SOVA_UDSПолныйОтветСервераВРезультатеОплаты": "MEDIUMTEXT",
    "SOVA_UDSРассчитанныйПроцентСкидки": "DECIMAL(18,4)",
    "SOVA_UDSРасчетДополнительногоБонуса": "MEDIUMTEXT",
    "SOVA_UDSСписываемыеБаллы": "VARCHAR(50)",
    "SOVA_UDSСуммаБезСкидки": "DECIMAL(18,2)",
    "SOVA_UDSСуммаДополнительногоНачисления": "DECIMAL(18,2)",

    "ВидОперации": "VARCHAR(50)",
    "ДенежныйСчет_Key": "CHAR(36)",
    "ДенежныйСчетБезнал_Key": "CHAR(36)",
    "ДенежныйСчетКредит_Key": "CHAR(36)",
    "ДисконтнаяКарточка_Key": "CHAR(36)",
    "КассирИНН": "VARCHAR(50)",
    "КассирФИО": "VARCHAR(255)",
    "КассоваяСмена_Key": "CHAR(36)",
    "КодАвторизации": "VARCHAR(100)",
    "Комментарий": "TEXT",
    "КорректировкаПоЛечению": "TINYINT(1)",
    "МДЛПИД": "VARCHAR(100)",
    "НеПередаватьКассира": "TINYINT(1)",
    "НомерПлатежнойКарты": "VARCHAR(100)",
    "НомерЧекаЭТ": "VARCHAR(100)",
    "ОкруглятьИтогЧека": "TINYINT(1)",
    "Организация_Key": "CHAR(36)",
    "Основание_Key": "CHAR(36)",
    "Ответственный_Key": "CHAR(36)",
    "ОтправлятьEmail": "TINYINT(1)",
    "ОтправлятьСМС": "TINYINT(1)",
    "Подразделение_Key": "CHAR(36)",
    "Сдача": "DECIMAL(18,2)",
    "СистемаНалогообложения": "VARCHAR(50)",
    "Состояние": "VARCHAR(50)",
    "СпособОкругленияИтогаЧека": "INT",
    "СсылочныйНомер": "VARCHAR(100)",
    "СуммаДокумента": "DECIMAL(18,2)",
    "СуммаОплатыБезнал": "DECIMAL(18,2)",
    "СуммаОплатыБонусами": "DECIMAL(18,2)",
    "СуммаОплатыКредитом": "DECIMAL(18,2)",
    "СуммаОплатыНал": "DECIMAL(18,2)",
    "СуммаТорговойУступки": "DECIMAL(18,2)",
    "ТипЦен_Key": "CHAR(36)",
    "УказанныйEmail": "VARCHAR(255)",
    "УказанныйТелефон": "VARCHAR(50)",
    "ФискальныйНомерЧека": "VARCHAR(100)",
    "Электронно": "TINYINT(1)",

    "MistyLoyalty_ПараметрыОперации_Type": "VARCHAR(100)",
    "MistyLoyalty_ПараметрыОперации_Base64Data": "MEDIUMTEXT",

    "ЭквайрИД": "VARCHAR(100)",
    "ТерминалИД": "VARCHAR(100)",
    "ПлатежнаяСистемаЭТ": "VARCHAR(100)",
    "СсылочныйНомерОснования": "VARCHAR(100)",
    "MistyLoyaltyOperationID": "VARCHAR(100)",
    "СуммаВключаетНДС": "TINYINT(1)",
    "ДокументБезНДС": "TINYINT(1)",
    "ЭквайрНаименование": "VARCHAR(255)",
    "ДругоеСредствоОплаты_Key": "CHAR(36)",
    "Контрагент_Key": "CHAR(36)",
    "Карточка_Key": "CHAR(36)",
    "ДенежныйСчетБезналДСО_Key": "CHAR(36)",
    "СуммаБезналДСО": "DECIMAL(18,2)",
    "ЧасоваяЗона": "INT",
    "Проверен": "TINYINT(1)",
}
# ======================================================

def log(msg): print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")

def mysql_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def ensure_table_and_schema(conn):
    # 1) Базова таблиця (мінімум)
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE}` (
              `Ref_Key` CHAR(36) NOT NULL,
              `DataVersion` VARCHAR(50) NOT NULL,
              `DeletionMark` TINYINT(1) NOT NULL DEFAULT 0,
              `Number` VARCHAR(20) NOT NULL DEFAULT '',
              `Date` DATETIME NOT NULL,
              `Posted` TINYINT(1) NOT NULL DEFAULT 0,
              `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (`Ref_Key`),
              KEY `ix_Date` (`Date`),
              KEY `ix_Number` (`Number`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
    conn.commit()

    # 2) Автодобудова відсутніх колонок за SELECT_FIELDS
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{TABLE}`")
        existing = {row["Field"] for row in cur.fetchall()}

    missing = [c for c in SELECT_FIELDS if c not in existing]
    if missing:
        alters = []
        for c in missing:
            sqltype = COLUMN_TYPES.get(c, "TEXT")
            alters.append(f"ADD COLUMN `{c}` {sqltype} NULL")
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE `{TABLE}` " + ", ".join(alters))
        conn.commit()
        log(f"Schema auto-migrated: added {len(missing)} columns")

def get_start_date(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(`Date`) AS max_dt FROM `{TABLE}`")
        row = cur.fetchone()
    if row and row["max_dt"]:
        return (row["max_dt"] - dt.timedelta(days=DAYS_BACK)).replace(microsecond=0)
    return dt.datetime.fromisoformat(START_IF_EMPTY + "T00:00:00")

def delete_range(conn, start_dt):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM `{TABLE}` WHERE `Date` >= %s", (start_dt,))
    conn.commit()

def preload_versions(conn):
    versions = {}
    with conn.cursor() as cur:
        cur.execute(f"SELECT `Ref_Key`,`DataVersion` FROM `{TABLE}`")
        for r in cur.fetchall():
            versions[r["Ref_Key"]] = r["DataVersion"]
    return versions

def odata_fetch_all(start_dt):
    base = os.getenv("ODATA_URL").rstrip("/") + f"/{ENTITY}"
    auth = (os.getenv("ODATA_USER"), os.getenv("ODATA_PASSWORD"))
    select = ",".join(SELECT_FIELDS)
    filter_q = f"Date ge datetime'{start_dt:%Y-%m-%dT%H:%M:%S}'"
    orderby = "Date,Ref_Key"

    all_rows, skip, batch_idx = [], 0, 1
    while True:
        url = f"{base}?$format=json&$orderby={orderby}&$select={select}&$filter={filter_q}&$top={BATCH_SIZE}&$skip={skip}"
        resp = requests.get(url, auth=auth, timeout=120)
        if not resp.ok:
            raise RuntimeError(f"OData HTTP {resp.status_code}: {resp.text[:500]}")
        payload = resp.json()
        rows = payload.get("value", [])
        if not rows:
            log(f"[BATCH {batch_idx}] fetched=0 (done)")
            break

        # лог по діапазону дат у порції
        dts = [r.get("Date") for r in rows if r.get("Date")]
        dt_min = min(dts) if dts else None
        dt_max = max(dts) if dts else None
        log(f"[BATCH {batch_idx}] fetched={len(rows)} | dates {dt_min} .. {dt_max}")

        all_rows.extend(rows)
        if len(rows) < BATCH_SIZE:
            break
        skip += BATCH_SIZE
        batch_idx += 1
        time.sleep(SLEEP_SECONDS)

    return all_rows

def normalize_row(r):
    def b(x):
        if isinstance(x, bool): return int(x)
        if x in (0, 1, None): return x
        return int(bool(x))
    def dec(x):
        if x in (None, ""): return None
        try: return Decimal(str(x))
        except Exception: return None

    out = {k: r.get(k) for k in SELECT_FIELDS}

    # bool → tinyint
    for k in ["DeletionMark","Posted","SOVA_UDSИспользоватьДополнительныйБонус",
              "SOVA_UDSОперацияЗарегистрированаНаСервере","ОкруглятьИтогЧека",
              "ОтправлятьEmail","ОтправлятьСМС","Электронно",
              "СуммаВключаетНДС","ДокументБезНДС","НеПередаватьКассира",
              "КорректировкаПоЛечению","Проверен"]:
        if k in out: out[k] = b(out[k])

    # дата
    if out.get("Date"):
        out["Date"] = out["Date"].replace("T", " ")

    # decimals
    for k in ["SOVA_UDSdiscountRate","SOVA_UDSВсяСуммаБезСкидки","SOVA_UDSНакопленоБаллов",
              "SOVA_UDSРассчитанныйПроцентСкидки","SOVA_UDSСуммаБезСкидки",
              "SOVA_UDSСуммаДополнительногоНачисления","Сдача","СуммаДокумента",
              "СуммаОплатыБезнал","СуммаОплатыБонусами","СуммаОплатыКредитом",
              "СуммаОплатыНал","СуммаТорговойУступки","СуммаБезналДСО"]:
        if k in out: out[k] = dec(out[k])
    return out

def insert_many(conn, rows):
    if not rows: return (0, 0, 0)
    cols = SELECT_FIELDS
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(f"`{c}`" for c in cols)
    sql_ins = f"INSERT INTO `{TABLE}` ({collist}) VALUES ({placeholders}) " \
              f"ON DUPLICATE KEY UPDATE " + ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols if c != "Ref_Key"])
    with conn.cursor() as cur:
        # виконуємо батчами по 1000
        for i in range(0, len(rows), 1000):
            vals = [tuple(r.get(c) for c in cols) for r in rows[i:i+1000]]
            cur.executemany(sql_ins, vals)
    conn.commit()
    return (len(rows), 0, 0)

def upsert_by_dataversion(conn, rows):
    if not rows: return (0, 0, 0)
    existing = preload_versions(conn)
    cols = SELECT_FIELDS
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(f"`{c}`" for c in cols)
    sql_upsert = f"INSERT INTO `{TABLE}` ({collist}) VALUES ({placeholders}) " \
                 f"ON DUPLICATE KEY UPDATE " \
                 + ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols if c != "Ref_Key"])

    ins = upd = skip = 0
    batch = []
    for r in rows:
        ref = r["Ref_Key"]; dv = r["DataVersion"]
        if ref in existing:
            if existing[ref] == dv:
                skip += 1
                continue
            upd += 1
        else:
            ins += 1
        batch.append(tuple(r.get(c) for c in cols))

    with conn.cursor() as cur:
        for i in range(0, len(batch), 1000):
            cur.executemany(sql_upsert, batch[i:i+1000])
    conn.commit()
    return (ins, upd, skip)

def main():
    load_dotenv()
    conn = mysql_conn()
    try:
        ensure_table_and_schema(conn)

        start_dt = get_start_date(conn)
        log(f"Start ETL: {ENTITY} -> {TABLE}")
        log(f"BATCH_SIZE={BATCH_SIZE}, SLEEP_SECONDS={SLEEP_SECONDS}, DAYS_BACK={DAYS_BACK}, LOAD_MODE={LOAD_MODE}")
        log(f"Start date (filter): {start_dt}")

        if LOAD_MODE == "rewrite_range":
            log("Deleting existing records in range ...")
            delete_range(conn, start_dt)

        raw = odata_fetch_all(start_dt)
        log(f"Total fetched: {len(raw)}")

        norm = [normalize_row(r) for r in raw]

        if LOAD_MODE == "rewrite_range":
            ins, upd, skip = insert_many(conn, norm)
        else:
            ins, upd, skip = upsert_by_dataversion(conn, norm)

        log(f"Done. inserted={ins} updated={upd} skipped={skip}")
    finally:
        try: conn.close()
        except Exception: pass

if __name__ == "__main__":
    main()
