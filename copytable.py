#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# тимчасовий скрипт для переносу таблиць банка на Хетцер з Україна.ком.юа

import os
import sys
import time
import re
import argparse
import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pymysql
from dotenv import load_dotenv

# ───────────── ЛОГИ ─────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mysql_copy")

# ───────────── CLI ─────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Одноразове копіювання таблиць між MySQL із санітизацією нуль-дат і м'яким керуванням тригерами."
    )
    p.add_argument("--tables", nargs="+",
                   default=["bnk_privat_balance", "bnk_trazact_prvt", "bnk_trazact_prvt_ekv"],
                   help="Список таблиць для копіювання.")
    p.add_argument("--no-truncate", action="store_true", help="Не очищати цільову таблицю перед копіюванням.")
    p.add_argument("--batch-size", type=int, default=1000, help="Розмір батча для читання/вставки.")
    p.add_argument("--sleep", type=float, default=0.0, help="Пауза (сек) між батчами.")
    p.add_argument("--dry-run", action="store_true", help="Лише підрахунок і перевірки, без вставок.")
    p.add_argument("--skip-triggers", action="store_true", help="Не чіпати тригери (не DROP і не CREATE).")
    return p.parse_args()

# ───────────── ENV ─────────────
# можна вказати шлях до .env через ENV_FILE, інакше шукає ./.env
load_dotenv(dotenv_path=os.environ.get("ENV_FILE", ".env"))

# Материнська (джерело) — ukraine.com.ua
SRC = {
    "host": os.getenv("SRC_DB_HOST", "wealth0.mysql.tools"),
    "port": int(os.getenv("SRC_DB_PORT", "3306")),
    "user": os.getenv("SRC_DB_USER", "wealth0_raid"),
    "password": os.getenv("SRC_DB_PASSWORD", "n5yM5ZT87z"),
    "db": os.getenv("SRC_DB_DATABASE", "wealth0_analytics"),
}

# Дочірня (ціль) — локальна
DST = {
    "host": os.getenv("DST_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DST_DB_PORT", "3306")),
    "user": os.getenv("DST_DB_USER", "olexii_raid"),
    "password": os.getenv("DST_DB_PASSWORD", "Z4vBrpT7@9uLMxWg"),
    "db": os.getenv("DST_DB_DATABASE", "petwealth"),
}

# ───────────── DB helpers ─────────────
@contextmanager
def mysql_conn(cfg: dict):
    conn = pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["db"],
        autocommit=False,
        charset="utf8mb4",
        use_unicode=True,
        cursorclass=pymysql.cursors.Cursor,
    )
    try:
        yield conn
    finally:
        conn.close()

def get_columns_ordered(cur, table: str):
    cur.execute(f"SHOW COLUMNS FROM `{table}`;")
    cols = [row[0] for row in cur.fetchall()]
    if not cols:
        raise RuntimeError(f"Не вдалося отримати колонки таблиці {table}")
    return cols

def count_rows(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM `{table}`;")
    return int(cur.fetchone()[0])

def truncate_table(cur, table: str):
    cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    cur.execute(f"TRUNCATE TABLE `{table}`;")
    cur.execute("SET FOREIGN_KEY_CHECKS=1;")

def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (schema, table),
    )
    return cur.fetchone() is not None

def get_date_like_columns(cur, schema: str, table: str):
    """Лише date/datetime/timestamp з ЦІЛЬОВОЇ схеми."""
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
          AND DATA_TYPE IN ('date','datetime','timestamp')
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]

# ───────────── НУЛЬ-ДАТИ ─────────────
ZERO_PREFIXES = ("0000-00-00",)

def looks_like_zero_date(v) -> bool:
    if v is None:
        return False
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="ignore")
        except Exception:
            return False
    if isinstance(v, str):
        s = v.strip()
        for pref in ZERO_PREFIXES:
            if s.startswith(pref):
                return True
    return False

def sanitize_zero_dates(rows, cols, date_cols_set):
    """Повертає (нові_ряди, к-ть_замін) — міняє лише у дата-колонках."""
    if not rows or not date_cols_set:
        return rows, 0
    idx = {c: i for i, c in enumerate(cols)}
    target_idx = [idx[c] for c in cols if c in date_cols_set]
    changed = 0
    out = []
    for row in rows:
        r = list(row)
        touched = False
        for i in target_idx:
            v = r[i]
            if looks_like_zero_date(v):
                r[i] = None
                changed += 1
                touched = True
        out.append(tuple(r) if touched else row)
    return out, changed

# ───────────── ТРИГЕРИ (DST) ─────────────
def dump_triggers_for_table(cur, schema: str, table: str):
    cur.execute(
        """
        SELECT TRIGGER_NAME
        FROM information_schema.TRIGGERS
        WHERE TRIGGER_SCHEMA=%s AND EVENT_OBJECT_TABLE=%s
        """,
        (schema, table),
    )
    names = [r[0] for r in cur.fetchall()]
    dump = {}
    for trg in names:
        cur.execute(f"SHOW CREATE TRIGGER `{trg}`")
        row = cur.fetchone()
        create_sql = None
        if row and len(row) >= 3 and isinstance(row[2], str):
            create_sql = row[2]
        else:
            for v in row[::-1]:
                if isinstance(v, str) and v.strip().upper().startswith("CREATE "):
                    create_sql = v
                    break
        if not create_sql:
            raise RuntimeError(f"Не вдалося отримати CREATE TRIGGER для {trg}")
        dump[trg] = create_sql
    return dump

def drop_triggers(cur, triggers_dump: dict):
    for trg in triggers_dump.keys():
        cur.execute(f"DROP TRIGGER IF EXISTS `{trg}`")

def recreate_triggers_current_user(cur, triggers_dump: dict):
    for trg, create_sql in triggers_dump.items():
        stmt = create_sql.strip().rstrip(";")
        # будь-який старий DEFINER -> CURRENT_USER
        stmt = re.sub(r"DEFINER\s*=\s*[^ ]+\s", "DEFINER=CURRENT_USER ", stmt, flags=re.IGNORECASE)
        cur.execute(stmt)

def save_triggers_sql(triggers_dump: dict, table: str, schema: str, out_dir: str = "./trigger_dumps") -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"restore_triggers_{schema}_{table}_{ts}.sql")
    lines = []
    for trg, create_sql in triggers_dump.items():
        stmt = create_sql.strip().rstrip(";")
        stmt = re.sub(r"DEFINER\s*=\s*[^ ]+\s", "DEFINER=CURRENT_USER ", stmt, flags=re.IGNORECASE)
        lines.append(stmt + ";\n")
    with open(path, "w", encoding="utf-8") as f:
        f.write("-- AUTO-GENERATED by copy_mysql_tables.py\n")
        f.write(f"-- schema: {schema}, table: {table}\n\n")
        for ln in lines:
            f.write(ln)
    return path

# ───────────── ОСНОВНА ЛОГІКА ─────────────
def copy_table(src_conn, dst_conn, table: str, batch_size: int, sleep_sec: float,
               do_truncate: bool, dry_run: bool, skip_triggers: bool):
    with src_conn.cursor() as src_cur, dst_conn.cursor() as dst_cur:
        # існування
        if not table_exists(src_cur, SRC["db"], table):
            raise RuntimeError(f"У вихідній БД немає таблиці `{table}`")
        if not table_exists(dst_cur, DST["db"], table):
            raise RuntimeError(f"У цільовій БД немає таблиці `{table}`")

        # тригери (тимчасово вимкнути)
        triggers_dump = {}
        if not dry_run and not skip_triggers:
            try:
                triggers_dump = dump_triggers_for_table(dst_cur, schema=DST["db"], table=table)
                if triggers_dump:
                    log.info(f"[{table}] Знайдено тригерів: {len(triggers_dump)} — тимчасово вимикаю (DROP)…")
                    drop_triggers(dst_cur, triggers_dump)
                    dst_conn.commit()
            except Exception as e:
                log.warning(f"[{table}] Не вдалося обробити тригери: {e} — продовжую без вимкнення.")

        # порядок колонок (за джерелом; структури ідентичні)
        cols = get_columns_ordered(src_cur, table)
        cols_list = ", ".join(f"`{c}`" for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))

        # дата-колонки в ЦІЛІ — тільки їх санітизуємо
        date_cols = set(get_date_like_columns(dst_cur, schema=DST["db"], table=table))

        total_src = count_rows(src_cur, table)
        log.info(f"[{table}] Рядків у джерелі: {total_src}")

        if not dry_run and do_truncate:
            log.info(f"[{table}] TRUNCATE цільової таблиці …")
            truncate_table(dst_cur, table)
            dst_conn.commit()

        if dry_run:
            log.info(f"[{table}] DRY-RUN: вставки пропущено.")
            return total_src, 0, 0

        # копіювання
        log.info(f"[{table}] Починаю копіювання батчами по {batch_size} …")
        src_cur.execute(f"SELECT {cols_list} FROM `{table}`")
        insert_sql = f"INSERT INTO `{table}` ({cols_list}) VALUES ({placeholders})"

        inserted = 0
        zero_fixed_total = 0

        while True:
            rows = src_cur.fetchmany(batch_size)
            if not rows:
                break
            # санітизація нуль-дат
            rows_sanitized, zero_fixed = sanitize_zero_dates(rows, cols, date_cols)
            zero_fixed_total += zero_fixed

            # вставка
            dst_cur.executemany(insert_sql, rows_sanitized)
            dst_conn.commit()

            inserted += len(rows_sanitized)
            log.info(f"[{table}] +{len(rows_sanitized)} (усього {inserted}/{total_src}), виправлено нуль-дат у полях: {zero_fixed}")

            if sleep_sec > 0:
                time.sleep(sleep_sec)

        # повернути тригери (м'яко)
        if triggers_dump and not skip_triggers:
            try:
                log.info(f"[{table}] Відновлюю тригери з DEFINER=CURRENT_USER …")
                recreate_triggers_current_user(dst_cur, triggers_dump)
                dst_conn.commit()
                log.info(f"[{table}] Тригери відновлено: {len(triggers_dump)}")
            except pymysql.err.OperationalError as e:
                if e.args and e.args[0] == 1419:
                    dump_path = save_triggers_sql(triggers_dump, table=table, schema=DST["db"])
                    log.error(
                        f"[{table}] ERROR 1419 (binlog увімкнено, бракує SUPER). Тригери НЕ відновлено. "
                        f"Збережено SQL для ручного відновлення: {dump_path}\n"
                        f"Рішення: SET GLOBAL log_bin_trust_function_creators=1; потім виконати файл."
                    )
                else:
                    log.warning(f"[{table}] Помилка відновлення тригерів: {e} — пропускаю.")

        dst_count = count_rows(dst_cur, table)
        log.info(f"[{table}] Готово. У цілі: {dst_count} рядків. Виправлено нуль-дат: {zero_fixed_total}")
        return total_src, inserted, zero_fixed_total

def main():
    args = parse_args()

    log.info("=== Джерело (материнська) ===")
    log.info(f"{SRC['user']}@{SRC['host']}:{SRC['port']} / {SRC['db']}")
    log.info("=== Призначення (дочірня) ===")
    log.info(f"{DST['user']}@{DST['host']}:{DST['port']} / {DST['db']}")
    log.info(f"Таблиці: {', '.join(args.tables)}")
    log.info(f"TRUNCATE: {not args.no_truncate}, BATCH_SIZE={args.batch_size}, SLEEP={args.sleep}, DRY_RUN={args.dry_run}, SKIP_TRIGGERS={args.skip_triggers}")

    with mysql_conn(SRC) as src_conn, mysql_conn(DST) as dst_conn:
        # інформативно
        with src_conn.cursor() as c:
            c.execute("SELECT CURRENT_USER(), USER(), @@version")
            cu, u, ver = c.fetchone()
            log.info(f"SRC as CURRENT_USER={cu}, USER={u}, ver={ver}")
        with dst_conn.cursor() as c:
            c.execute("SELECT CURRENT_USER(), USER(), @@version, @@SESSION.sql_mode")
            cu, u, ver, sm = c.fetchone()
            log.info(f"DST as CURRENT_USER={cu}, USER={u}, ver={ver}")
            log.info(f"DST sql_mode={sm}")

        # запобіжник
        if SRC["host"] == DST["host"] and SRC["db"] == DST["db"]:
            log.error("SRC і DST вказують на один і той самий сервер/БД — перериваю.")
            sys.exit(2)

        totals = []
        for t in args.tables:
            try:
                total_src, inserted, fixed = copy_table(
                    src_conn=src_conn,
                    dst_conn=dst_conn,
                    table=t,
                    batch_size=args.batch_size,
                    sleep_sec=args.sleep,
                    do_truncate=(not args.no_truncate),
                    dry_run=args.dry_run,
                    skip_triggers=args.skip_triggers,
                )
                totals.append((t, total_src, inserted, fixed))
            except Exception as e:
                log.exception(f"[{t}] Помилка копіювання: {e}")
                # не валимо весь прогін — переходимо до наступної таблиці
                totals.append((t, -1, -1, -1))
                continue

    log.info("=== Підсумок ===")
    for t, total_src, inserted, fixed in totals:
        if total_src == -1:
            log.info(f"{t}: пропущено через помилку (дивись вище).")
        elif args.dry_run:
            log.info(f"{t}: джерело={total_src}, вставок (dry-run)=0, виправлено нуль-дат=0")
        else:
            log.info(f"{t}: джерело={total_src}, вставлено={inserted}, виправлено нуль-дат={fixed}")

if __name__ == "__main__":
    main()
