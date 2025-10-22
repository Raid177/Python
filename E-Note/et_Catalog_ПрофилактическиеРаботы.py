#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import sys
import logging
from typing import List, Dict

import requests
from requests.auth import HTTPBasicAuth
import mysql.connector as mysql
from dotenv import load_dotenv

# ======================== ПАРАМЕТРИ СКРИПТА ===========================
BATCH_SIZE = 1000         # скільки рядків за раз тягнемо з OData
SLEEP_SECONDS = 1.0       # пауза між HTTP-запитами до OData
DELETE_BEFORE_LOAD = True # True => TRUNCATE + INSERT; False => UPSERT лише якщо змінився DataVersion

ODATA_ENTITY = "Catalog_ПрофилактическиеРаботы"
TABLE_NAME  = "et_Catalog_ПрофилактическиеРаботы"

SELECT_FIELDS = [
    "Ref_Key",
    "DataVersion",
    "DeletionMark",
    "Parent_Key",
    "IsFolder",
    "Code",
    "Description",
    "Контролировать",
    "Тип_Key",
    "Пожизненно",
]
# =====================================================================

# Чистий INSERT (для режиму TRUNCATE)
INSERT_SQL = f"""
INSERT INTO `{TABLE_NAME}`
(`Ref_Key`,`DataVersion`,`DeletionMark`,`Parent_Key`,`IsFolder`,`Code`,
 `Description`,`Контролировать`,`Тип_Key`,`Пожизненно`)
VALUES
(%(Ref_Key)s,%(DataVersion)s,%(DeletionMark)s,%(Parent_Key)s,%(IsFolder)s,%(Code)s,
 %(Description)s,%(Контролировать)s,%(Тип_Key)s,%(Пожизненно)s);
"""

# UPSERT: оновлюємо РЯДОК лише якщо нова DataVersion відрізняється від поточної
# Всі поля (і updated_at) змінюються тільки при зміні версії.
UPSERT_SQL = f"""
INSERT INTO `{TABLE_NAME}`
(`Ref_Key`,`DataVersion`,`DeletionMark`,`Parent_Key`,`IsFolder`,`Code`,
 `Description`,`Контролировать`,`Тип_Key`,`Пожизненно`)
VALUES
(%(Ref_Key)s,%(DataVersion)s,%(DeletionMark)s,%(Parent_Key)s,%(IsFolder)s,%(Code)s,
 %(Description)s,%(Контролировать)s,%(Тип_Key)s,%(Пожизненно)s)
ON DUPLICATE KEY UPDATE
  DataVersion = IF(VALUES(DataVersion)<>DataVersion, VALUES(DataVersion), DataVersion),
  DeletionMark = IF(VALUES(DataVersion)<>DataVersion, VALUES(DeletionMark), DeletionMark),
  Parent_Key   = IF(VALUES(DataVersion)<>DataVersion, VALUES(Parent_Key), Parent_Key),
  IsFolder     = IF(VALUES(DataVersion)<>DataVersion, VALUES(IsFolder), IsFolder),
  Code         = IF(VALUES(DataVersion)<>DataVersion, VALUES(Code), Code),
  Description  = IF(VALUES(DataVersion)<>DataVersion, VALUES(Description), Description),
  `Контролировать` = IF(VALUES(DataVersion)<>DataVersion, VALUES(`Контролировать`), `Контролировать`),
  `Тип_Key`    = IF(VALUES(DataVersion)<>DataVersion, VALUES(`Тип_Key`), `Тип_Key`),
  `Пожизненно` = IF(VALUES(DataVersion)<>DataVersion, VALUES(`Пожизненно`), `Пожизненно`),
  updated_at   = IF(VALUES(DataVersion)<>DataVersion, CURRENT_TIMESTAMP, updated_at);
"""

# -------------------- ЛОГУВАННЯ --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("enote.catalog.loader")


def load_env():
    env_path = "/root/Python/.env"
    if not os.path.exists(env_path):
        log.error("Не знайдений .env файл: %s", env_path)
        sys.exit(1)
    load_dotenv(env_path)

    odata_user = os.getenv("ODATA_USER")
    odata_pass = os.getenv("ODATA_PASSWORD")
    odata_url  = os.getenv("ODATA_URL")

    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = int(os.getenv("DB_PORT", "3306"))
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_DATABASE")

    missing = [k for k, v in {
        "ODATA_USER": odata_user,
        "ODATA_PASSWORD": odata_pass,
        "ODATA_URL": odata_url,
        "DB_USER": db_user,
        "DB_PASSWORD": db_pass,
        "DB_DATABASE": db_name,
    }.items() if not v]
    if missing:
        log.error("В .env відсутні змінні: %s", ", ".join(missing))
        sys.exit(1)

    return {
        "odata_user": odata_user,
        "odata_pass": odata_pass,
        "odata_url": odata_url.rstrip("/") + "/",
        "db_host": db_host,
        "db_port": db_port,
        "db_user": db_user,
        "db_pass": db_pass,
        "db_name": db_name,
    }


def db_connect(cfg):
    try:
        cn = mysql.connect(
            host=cfg["db_host"],
            port=cfg["db_port"],
            user=cfg["db_user"],
            password=cfg["db_pass"],
            database=cfg["db_name"],
            autocommit=False,
            charset="utf8mb4",
            collation="utf8mb4_unicode_ci",
        )
        with cn.cursor() as cur:
            cur.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;")
        return cn
    except mysql.Error as e:
        log.error("Помилка з'єднання з БД: %s", e)
        sys.exit(1)


def truncate_table(cn):
    with cn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE `{TABLE_NAME}`;")
    cn.commit()
    log.info("Таблицю очищено (TRUNCATE).")


def fetch_batch(session: requests.Session, base_url: str, auth: HTTPBasicAuth,
                top: int, skip: int) -> List[Dict]:
    entity_url = f"{base_url}{ODATA_ENTITY}"
    params = {
        "$format": "json",
        "$select": ",".join(SELECT_FIELDS),
        "$orderby": "Ref_Key",
        "$top": str(top),
        "$skip": str(skip),
    }
    r = session.get(entity_url, params=params, auth=auth, timeout=60)
    r.raise_for_status()
    data = r.json()
    records = data.get("value", data if isinstance(data, list) else [])
    return records


def normalize_row(row: Dict) -> Dict:
    out = {k: row.get(k) for k in SELECT_FIELDS}
    for bkey in ("DeletionMark", "IsFolder", "Контролировать", "Пожизненно"):
        out[bkey] = 1 if bool(out.get(bkey)) else 0
    if not out.get("Parent_Key"):
        out["Parent_Key"] = "00000000-0000-0000-0000-000000000000"
    return out


def insert_batch(cn, rows: List[Dict], upsert: bool):
    if not rows:
        return
    rows_norm = [normalize_row(r) for r in rows]
    sql = UPSERT_SQL if upsert else INSERT_SQL
    with cn.cursor() as cur:
        cur.executemany(sql, rows_norm)


def main():
    cfg = load_env()

    log.info("=== Старт завантаження %s -> %s.%s ===",
             ODATA_ENTITY, cfg["db_name"], TABLE_NAME)
    log.info("BATCH_SIZE=%s | SLEEP_SECONDS=%.1f | DELETE_BEFORE_LOAD=%s",
             BATCH_SIZE, SLEEP_SECONDS, DELETE_BEFORE_LOAD)

    cn = db_connect(cfg)
    if DELETE_BEFORE_LOAD:
        truncate_table(cn)

    base_url = cfg["odata_url"]
    auth = HTTPBasicAuth(cfg["odata_user"], cfg["odata_pass"])
    session = requests.Session()

    total = 0
    skip = 0
    batch_no = 0

    while True:
        batch_no += 1
        t0 = time.time()
        try:
            rows = fetch_batch(session, base_url, auth, BATCH_SIZE, skip)
        except requests.HTTPError as e:
            log.error("HTTP %s при отриманні пачки skip=%s: %s",
                      getattr(e.response, 'status_code', '?'), skip, e)
            sys.exit(2)
        except Exception as e:
            log.error("Помилка мережі/парсингу при skip=%s: %s", skip, e)
            sys.exit(2)

        n = len(rows)
        if n == 0:
            log.info("Порожня відповідь при skip=%s. Завершуємо.", skip)
            break

        first_key = rows[0].get("Ref_Key")
        last_key  = rows[-1].get("Ref_Key")

        try:
            insert_batch(cn, rows, upsert=(not DELETE_BEFORE_LOAD))
            cn.commit()
        except mysql.Error as e:
            cn.rollback()
            log.error("DB помилка на вставці (skip=%s, batch=%s): %s", skip, batch_no, e)
            sys.exit(3)

        total += n
        took = time.time() - t0
        mode = "INSERT" if DELETE_BEFORE_LOAD else "UPSERT(by DataVersion)"
        log.info("[%-22s] %4d рядків (skip=%s) | Ref_Key: %s .. %s | %.2fs",
                 mode, n, skip, first_key, last_key, took)

        if n < BATCH_SIZE:
            break

        skip += BATCH_SIZE
        time.sleep(SLEEP_SECONDS)

    log.info("=== Готово. Завантажено (оброблено): %d рядків у %s ===", total, TABLE_NAME)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("Перервано користувачем (Ctrl+C).")
        sys.exit(130)
