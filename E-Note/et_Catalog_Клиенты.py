#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, sys, base64
import requests
import mysql.connector
from urllib.parse import urljoin
from datetime import datetime
from dotenv import load_dotenv

# Конфіг
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1.0"))

# Поля, які беремо з OData (без масивів та navigationLinkUrl)
FIELDS = [
    "Ref_Key","DataVersion","DeletionMark","Parent_Key","IsFolder",
    "Code","Description",
    "ДопустимаяСуммаЗадолженности","ДопустимоеЧислоДнейЗадолженности",
    "Комментарий","КонтролироватьСуммуЗадолженности","КонтролироватьЧислоДнейЗадолженности",
    "НаименованиеКратко","ОсновноеКонтактноеЛицо_Key","Покупатель","Поставщик",
    "удалитьТипЦен_Key","ЮрФизЛицо","удалитьВалютаВзаиморасчетов_Key",
    "КонтактнаяИнформация","ГруппаПолучателейСкидки_Key",
    "ВрачКуратор","ВрачКуратор_Type","Организация_Key","Подразделение_Key",
    "SOVA_IDКлиента","SOVA_UDSUParticipantIDКлиента","SOVA_UDSUIDКлиента",
    "КоличествоДонаций","Подтвержден","Фонд","НенадежныйКлиент",
    "ОсновнойКонтрагент_Key","ID","VIP","Predefined","PredefinedDataName"
]

def get_env():
    load_dotenv("/root/Python/.env")
    env = {
        "ODATA_URL": os.getenv("ODATA_URL"),
        "ODATA_USER": os.getenv("ODATA_USER"),
        "ODATA_PASSWORD": os.getenv("ODATA_PASSWORD"),
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": int(os.getenv("DB_PORT", "3306")),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_DATABASE": os.getenv("DB_DATABASE"),
    }
    missing = [k for k,v in env.items() if not v]
    if missing:
        print(f"[ERR] Missing .env keys: {missing}", file=sys.stderr)
        sys.exit(1)
    return env

def connect_db(env):
    return mysql.connector.connect(
        host=env["DB_HOST"], port=env["DB_PORT"],
        user=env["DB_USER"], password=env["DB_PASSWORD"],
        database=env["DB_DATABASE"], autocommit=False, charset="utf8mb4"
    )

def fetch_batch(session, base_url, skip, top):
    params = {
        "$format": "json",
        "$orderby": "Ref_Key",
        "$skip": skip,
        "$top": top,
        "$select": ",".join(FIELDS)
    }
    url = urljoin(base_url, "Catalog_Клиенты")
    r = session.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data.get("value", [])

def normalize_row(r: dict):
    # перетворення типів там, де з OData приходить рядок "0"
    def to_int(x):
        try:
            return int(x)
        except Exception:
            return 0

    return {
        "Ref_Key": r.get("Ref_Key"),
        "DataVersion": r.get("DataVersion",""),
        "DeletionMark": bool(r.get("DeletionMark", False)),
        "Parent_Key": r.get("Parent_Key"),
        "IsFolder": bool(r.get("IsFolder", False)),
        "Code": r.get("Code",""),
        "Description": r.get("Description",""),
        "ДопустимаяСуммаЗадолженности": r.get("ДопустимаяСуммаЗадолженности", 0) or 0,
        "ДопустимоеЧислоДнейЗадолженности": to_int(r.get("ДопустимоеЧислоДнейЗадолженности", 0)),
        "Комментарий": r.get("Комментарий",""),
        "КонтролироватьСуммуЗадолженности": bool(r.get("КонтролироватьСуммуЗадолженности", False)),
        "КонтролироватьЧислоДнейЗадолженности": bool(r.get("КонтролироватьЧислоДнейЗадолженности", False)),
        "НаименованиеКратко": r.get("НаименованиеКратко",""),
        "ОсновноеКонтактноеЛицо_Key": r.get("ОсновноеКонтактноеЛицо_Key"),
        "Покупатель": bool(r.get("Покупатель", False)),
        "Поставщик": bool(r.get("Поставщик", False)),
        "удалитьТипЦен_Key": r.get("удалитьТипЦен_Key"),
        "ЮрФизЛицо": r.get("ЮрФизЛицо"),
        "удалитьВалютаВзаиморасчетов_Key": r.get("удалитьВалютаВзаиморасчетов_Key"),
        "КонтактнаяИнформация": r.get("КонтактнаяИнформация",""),
        "ГруппаПолучателейСкидки_Key": r.get("ГруппаПолучателейСкидки_Key"),
        "ВрачКуратор": r.get("ВрачКуратор",""),
        "ВрачКуратор_Type": r.get("ВрачКуратор_Type",""),
        "Организация_Key": r.get("Организация_Key"),
        "Подразделение_Key": r.get("Подразделение_Key"),
        "SOVA_IDКлиента": r.get("SOVA_IDКлиента",""),
        "SOVA_UDSUParticipantIDКлиента": r.get("SOVA_UDSUParticipantIDКлиента",""),
        "SOVA_UDSUIDКлиента": r.get("SOVA_UDSUIDКлиента",""),
        "КоличествоДонаций": to_int(r.get("КоличествоДонаций", 0)),
        "Подтвержден": bool(r.get("Подтвержден", False)),
        "Фонд": bool(r.get("Фонд", False)),
        "НенадежныйКлиент": bool(r.get("НенадежныйКлиент", False)),
        "ОсновнойКонтрагент_Key": r.get("ОсновнойКонтрагент_Key"),
        "ID": r.get("ID",""),
        "VIP": bool(r.get("VIP", False)),
        "Predefined": bool(r.get("Predefined", False)),
        "PredefinedDataName": r.get("PredefinedDataName",""),
    }

def upsert_batch(conn, rows):
    if not rows:
        return (0,0)
    cur = conn.cursor(dictionary=True)

    # 1) зчитуємо поточні DataVersion по ключах
    keys = tuple(r["Ref_Key"] for r in rows)
    placeholders = ",".join(["%s"]*len(keys))
    cur.execute(f"SELECT Ref_Key, DataVersion FROM et_Catalog_Клиенты WHERE Ref_Key IN ({placeholders})", keys)
    existing = {rec["Ref_Key"]: rec["DataVersion"] for rec in cur.fetchall()}

    to_insert = []
    to_update = []
    for r in rows:
        if r["Ref_Key"] not in existing:
            to_insert.append(r)
        elif existing[r["Ref_Key"]] != r["DataVersion"]:
            to_update.append(r)

    ins_sql = f"""
    INSERT INTO et_Catalog_Клиенты (
      {",".join([f"`{k}`" for k in rows[0].keys()])}
    ) VALUES (
      {",".join(["%s"]*len(rows[0]))}
    )
    """
    upd_sql = """
    UPDATE et_Catalog_Клиенты SET
      DataVersion=%s,
      DeletionMark=%s, Parent_Key=%s, IsFolder=%s,
      Code=%s, Description=%s,
      `ДопустимаяСуммаЗадолженности`=%s, `ДопустимоеЧислоДнейЗадолженности`=%s,
      `Комментарий`=%s, `КонтролироватьСуммуЗадолженности`=%s, `КонтролироватьЧислоДнейЗадолженности`=%s,
      `НаименованиеКратко`=%s, `ОсновноеКонтактноеЛицо_Key`=%s, `Покупатель`=%s, `Поставщик`=%s,
      `удалитьТипЦен_Key`=%s, `ЮрФизЛицо`=%s, `удалитьВалютаВзаиморасчетов_Key`=%s,
      `КонтактнаяИнформация`=%s, `ГруппаПолучателейСкидки_Key`=%s,
      `ВрачКуратор`=%s, `ВрачКуратор_Type`=%s, `Организация_Key`=%s, `Подразделение_Key`=%s,
      `SOVA_IDКлиента`=%s, `SOVA_UDSUParticipantIDКлиента`=%s, `SOVA_UDSUIDКлиента`=%s,
      `КоличествоДонаций`=%s, `Подтвержден`=%s, `Фонд`=%s, `НенадежныйКлиент`=%s,
      `ОсновнойКонтрагент_Key`=%s, `ID`=%s, `VIP`=%s, `Predefined`=%s, `PredefinedDataName`=%s
    WHERE Ref_Key=%s
    """

    ins_cnt = upd_cnt = 0

    # INSERT
    if to_insert:
        cur.executemany(
            ins_sql,
            [tuple(r[k] for k in rows[0].keys()) for r in to_insert]
        )
        ins_cnt = cur.rowcount

    # UPDATE
    if to_update:
        cur.executemany(
            upd_sql,
            [(
                r["DataVersion"], r["DeletionMark"], r["Parent_Key"], r["IsFolder"],
                r["Code"], r["Description"],
                r["ДопустимаяСуммаЗадолженности"], r["ДопустимоеЧислоДнейЗадолженности"],
                r["Комментарий"], r["КонтролироватьСуммуЗадолженности"], r["КонтролироватьЧислоДнейЗадолженности"],
                r["НаименованиеКратко"], r["ОсновноеКонтактноеЛицо_Key"], r["Покупатель"], r["Поставщик"],
                r["удалитьТипЦен_Key"], r["ЮрФизЛицо"], r["удалитьВалютаВзаиморасчетов_Key"],
                r["КонтактнаяИнформация"], r["ГруппаПолучателейСкидки_Key"],
                r["ВрачКуратор"], r["ВрачКуратор_Type"], r["Организация_Key"], r["Подразделение_Key"],
                r["SOVA_IDКлиента"], r["SOVA_UDSUParticipantIDКлиента"], r["SOVA_UDSUIDКлиента"],
                r["КоличествоДонаций"], r["Подтвержден"], r["Фонд"], r["НенадежныйКлиент"],
                r["ОсновнойКонтрагент_Key"], r["ID"], r["VIP"], r["Predefined"], r["PredefinedDataName"],
                r["Ref_Key"]
            ) for r in to_update]
        )
        upd_cnt = cur.rowcount

    conn.commit()
    cur.close()
    return ins_cnt, upd_cnt

def main():
    env = get_env()

    # HTTP сесія
    session = requests.Session()
    session.auth = (env["ODATA_USER"], env["ODATA_PASSWORD"])

    print(f"[INFO] SOURCE: {env['ODATA_URL']}Catalog_Клиенты")
    print(f"[INFO] TARGET: {env['DB_DATABASE']}.et_Catalog_Клиенты")
    print(f"[INFO] BATCH_SIZE={BATCH_SIZE}, SLEEP_SECONDS={SLEEP_SECONDS}")

    conn = connect_db(env)
    skip = 0
    total = ins_total = upd_total = 0

    while True:
        batch = fetch_batch(session, env["ODATA_URL"], skip, BATCH_SIZE)
        if not batch:
            break

        rows = [normalize_row(r) for r in batch]
        ins_cnt, upd_cnt = upsert_batch(conn, rows)

        total += len(rows)
        ins_total += ins_cnt
        upd_total += upd_cnt

        print(f"[FETCH] {len(rows)} записів (skip={skip}) | inserted={ins_cnt} updated={upd_cnt}")
        skip += BATCH_SIZE
        time.sleep(SLEEP_SECONDS)

    conn.close()
    print(f"[DONE] received={total}, inserted={ins_total}, updated={upd_total}")

if __name__ == "__main__":
    main()
