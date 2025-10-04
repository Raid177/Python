#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
truncate_zp_tables.py

Очищає (TRUNCATE) службові таблиці ЗП перед перезапуском пайплайну.
- Авторизація до MySQL береться з .env
- Перевіряє існування таблиць (скипає відсутні)
- На час операцій вимикає FOREIGN_KEY_CHECKS
- Лог у stdout (run_all підхопить)
"""

import os
import sys
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

try:
    import mysql.connector as mysql_driver
    from mysql.connector import Error as MySQLError
    DRIVER = "connector"
except ImportError:
    import pymysql as mysql_driver
    from pymysql import MySQLError
    DRIVER = "pymysql"


# Явно вказуємо шлях
ENV_PATH = "/root/Python/.env"
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    print(f"[WARN] .env не знайдено за шляхом {ENV_PATH}")

TABLES_TO_TRUNCATE = [
    "zp_collective_bonus",
    "zp_log_schedule_errors",
    "zp_sales_prem",
    "zp_sales_salary",
    "zp_summary",
    "zp_worktime",
    "zp_довСпівробітники",
    "zp_фктРівніСпівробітників",
    "zp_фктУмовиОплати",
   "zp_rule_eval_debug",
]

def env(var_name, default=None, required=False):
    val = os.getenv(var_name, default)
    if required and (val is None or val == ""):
        print(f"[ERROR] Missing required env var: {var_name}")
        sys.exit(2)
    return val

def table_exists(cursor, schema, table_name):
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (schema, table_name)
    )
    return cursor.fetchone() is not None

def main():
    load_dotenv()  # читає .env у поточній директорії або вище

    host = env("DB_HOST", required=True)
    port = int(env("DB_PORT", "3306"))
    user = env("DB_USER", required=True)
    password = env("DB_PASSWORD", required=True)
    database = env("DB_DATABASE", required=True)

    print(f"[INFO] Підключення до БД {user}@{host}:{port}/{database}")

    try:
        conn = mysql.connector.connect(
            host=host, port=port, user=user, password=password, database=database,
            autocommit=True
        )
    except mysql.connector.Error as e:
        print(f"[ERROR] Не вдалось підключитися до БД: {e}")
        sys.exit(2)

    try:
        cur = conn.cursor()
        # Вимикаємо перевірку зовнішніх ключів (бо TRUNCATE = DDL DROP/CREATE)
        cur.execute("SET FOREIGN_KEY_CHECKS=0;")

        truncated = []
        skipped = []
        failed = []

        for tbl in TABLES_TO_TRUNCATE:
            full_name = f"`{database}`.`{tbl}`"
            try:
                if not table_exists(cur, database, tbl):
                    print(f"[WARN] Таблиця відсутня, пропускаю: {full_name}")
                    skipped.append(tbl)
                    continue

                print(f"[RUN] TRUNCATE {full_name} ...")
                cur.execute(f"TRUNCATE TABLE {full_name};")
                print(f"[OK] Очищено: {full_name}")
                truncated.append(tbl)
            except mysql.connector.Error as e:
                print(f"[ERROR] Не вдалося очистити {full_name}: {e}")
                failed.append((tbl, str(e)))

        # Повертаємо вмикнення перевірок
        cur.execute("SET FOREIGN_KEY_CHECKS=1;")

        # Звіт
        print("-" * 40)
        print("[SUMMARY] Результати очищення:")
        if truncated:
            print("  Очищено:", ", ".join(truncated))
        if skipped:
            print("  Пропущено (немає таблиці):", ", ".join(skipped))
        if failed:
            print("  Помилки:")
            for t, msg in failed:
                print(f"    - {t}: {msg}")

        # Якщо були фейли — повернемо ненульовий код, щоб пайплайн зупинився
        if failed:
            sys.exit(1)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

if __name__ == "__main__":
    main()
