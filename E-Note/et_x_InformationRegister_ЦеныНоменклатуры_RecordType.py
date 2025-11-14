#!/usr/bin/env python3
# /root/Python/E-Note/et_x_InformationRegister_ЦеныНоменклатуры_RecordType.py
import os
import sys
import time
import subprocess
import mysql.connector
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

# ===== Налаштування =====
RUN_SOURCES = True   # ← запускати оновлення джерел перед агрегацією
BATCH_SIZE  = 1000   # вставка/апдейт за раз
DAYS_BACK   = 14     # відмотка від MAX(Period) цільової
SLEEP_SEC   = 0.0    # пауза між батчами

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "autocommit": False,   # самі керуємо комітами
}

SRC_SCRIPTS = [
    "et_Catalog_ЕдиницыИзмерения.py",
    "et_Catalog_Номенклатура.py",
    "et_InformationRegister_ЦеныНоменклатуры_RecordType.py",
]

def sh(script_path: Path) -> None:
    """Запустити підскрипт тим самим інтерпретатором, з .env у цій папці."""
    print(f"[RUN] {script_path.name} …")
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        env={**os.environ},          # .env вже підхоплений load_dotenv
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        # Показуємо хвіст помилки для діагностики
        print(proc.stdout)
        print(proc.stderr)
        raise RuntimeError(f"FAIL: {script_path.name} (rc={proc.returncode})")
    # Короткий релевантний хвіст stdout
    tail = "\n".join(proc.stdout.strip().splitlines()[-10:])
    if tail:
        print(tail)
    print(f"[OK ] {script_path.name}")

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def clamp_future(dt: datetime) -> datetime:
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt if dt <= now else now

def get_cutoff(conn_w):
    """Визначити старт по MAX(Period) у цілій (з клемпом майбутнього) і відмоткою."""
    cur = conn_w.cursor()
    cur.execute("SELECT MAX(Period) FROM `et+_InformationRegister_ЦеныНоменклатуры_RecordType`")
    row = cur.fetchone()
    cur.close()
    last = row[0] if row and row[0] else datetime(2024, 1, 1, tzinfo=timezone.utc)
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    last = clamp_future(last)
    start = last - timedelta(days=DAYS_BACK)
    return start, last

def aggregate():
    """Зведення з трьох таблиць-джерел у цільову."""
    conn_r = get_conn()  # SELECT
    conn_w = get_conn()  # INSERT/UPDATE

    start_from, last_in_target = get_cutoff(conn_w)
    print(f"[INFO] Цільова MAX(Period): {last_in_target}; стартуємо з: {start_from}")

    select_sql = """
        SELECT
            p.Period,
            p.Recorder,
            p.Recorder_Type,
            p.LineNumber,
            p.Active,
            p.ТипЦен_Key,
            p.Номенклатура_Key,
            p.ЕдиницаИзмерения_Key,
            p.Валюта_Key,
            p.Цена,
            CASE WHEN p.ЕдиницаИзмерения_Key = n.ЕдиницаХраненияОстатков_Key THEN 1 ELSE 0 END AS Is_БазоваяЕдиница,
            eu.Коэффициент AS Коэффициент,
            eu.Description AS Одиниця,
            eu_base.Description AS БазОДНазва
        FROM et_InformationRegister_ЦеныНоменклатуры_RecordType p
        LEFT JOIN et_Catalog_Номенклатура n
               ON p.Номенклатура_Key = n.Ref_Key
        LEFT JOIN et_Catalog_ЕдиницыИзмерения eu
               ON p.ЕдиницаИзмерения_Key = eu.Ref_Key
        LEFT JOIN et_Catalog_ЕдиницыИзмерения eu_base
               ON n.ЕдиницаХраненияОстатков_Key = eu_base.Ref_Key
        WHERE p.Period >= %s
        ORDER BY p.Period
    """

    insert_sql = """
        INSERT INTO `et+_InformationRegister_ЦеныНоменклатуры_RecordType` (
            Period, Recorder, Recorder_Type, LineNumber, Active,
            ТипЦен_Key, Номенклатура_Key, ЕдиницаИзмерения_Key, Валюта_Key, Цена,
            Is_БазоваяЕдиница, Коэффициент, Одиниця, БазОДНазва,
            created_at, updated_at
        ) VALUES (
            %(Period)s, %(Recorder)s, %(Recorder_Type)s, %(LineNumber)s, %(Active)s,
            %(ТипЦен_Key)s, %(Номенклатура_Key)s, %(ЕдиницаИзмерения_Key)s, %(Валюта_Key)s, %(Цена)s,
            %(Is_БазоваяЕдиница)s, %(Коэффициент)s, %(Одиниця)s, %(БазОДНазва)s,
            NOW(), NOW()
        )
        ON DUPLICATE KEY UPDATE
            Recorder = VALUES(Recorder),
            Recorder_Type = VALUES(Recorder_Type),
            LineNumber = VALUES(LineNumber),
            Active = VALUES(Active),
            ТипЦен_Key = VALUES(ТипЦен_Key),
            Номенклатура_Key = VALUES(Номенклатура_Key),
            ЕдиницаИзмерения_Key = VALUES(ЕдиницаИзмерения_Key),
            Валюта_Key = VALUES(Валюта_Key),
            Цена = VALUES(Цена),
            Is_БазоваяЕдиница = VALUES(Is_БазоваяЕдиница),
            Коэффициент = VALUES(Коэффициент),
            Одиниця = VALUES(Одиниця),
            БазОДНазва = VALUES(БазОДНазва),
            updated_at = NOW()
    """

    cur_r = conn_r.cursor(dictionary=True, buffered=False)
    cur_r.execute(select_sql, (start_from,))
    cur_w = conn_w.cursor()

    total = 0
    while True:
        rows = cur_r.fetchmany(BATCH_SIZE)
        if not rows:
            break

        params = [{
            "Period": r["Period"],
            "Recorder": r["Recorder"],
            "Recorder_Type": r["Recorder_Type"],
            "LineNumber": r["LineNumber"],
            "Active": r["Active"],
            "ТипЦен_Key": r["ТипЦен_Key"],
            "Номенклатура_Key": r["Номенклатура_Key"],
            "ЕдиницаИзмерения_Key": r["ЕдиницаИзмерения_Key"],
            "Валюта_Key": r["Валюта_Key"],
            "Цена": r["Цена"],
            "Is_БазоваяЕдиница": r["Is_БазоваяЕдиница"],
            "Коэффициент": r["Коэффициент"],
            "Одиниця": r["Одиниця"],
            "БазОДНазва": r["БазОДНазва"],
        } for r in rows]

        cur_w.executemany(insert_sql, params)
        conn_w.commit()

        total += len(rows)
        print(f"[INFO] Оброблено {total} записів...")
        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    cur_r.close()
    cur_w.close()
    conn_r.close()
    conn_w.close()
    print(f"[OK] Готово. Всього оброблено: {total}")

def main():
    # 0) (необов'язково) оновити джерела
    if RUN_SOURCES:
        for fname in SRC_SCRIPTS:
            sh(BASE_DIR / fname)

    # 1) агрегація
    aggregate()

if __name__ == "__main__":
    main()
