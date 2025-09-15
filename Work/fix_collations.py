#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
normalize_collation.py

Приводить БД та всі її таблиці/текстові колонки до колації utf8mb4_0900_ai_ci (MySQL 8+).
- Чіткий консольний лог.
- Авторизація через .env (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE).
"""

import os
import sys
import time
import mysql.connector
from dotenv import load_dotenv

# === Константи ===
TARGET_CHARSET   = "utf8mb4"
TARGET_COLLATION = "utf8mb4_0900_ai_ci"

TEXT_DATA_TYPES = {
    "char", "varchar",
    "tinytext", "text", "mediumtext", "longtext",
    "enum", "set"
}

# ========= ENV loader =========
ENV_PROFILE = os.getenv("ENV_PROFILE", "prod")  # dev | prod
ENV_PATHS = {
    "dev":  r"C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env",
    "prod": "/root/Python/_Acces/.env.prod",
}
ENV_PATH = os.getenv("ENV_PATH") or ENV_PATHS.get(ENV_PROFILE)
if ENV_PATH and os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)
else:
    load_dotenv(override=True)  # шукає .env у поточній папці

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"[ENV ERROR] Missing {name} in .env (path: {ENV_PATH or './.env'})", file=sys.stderr)
        sys.exit(1)
    return v

DB_HOST = require_env("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = require_env("DB_USER")
DB_PASSWORD = require_env("DB_PASSWORD")
DB_DATABASE = require_env("DB_DATABASE")

def connect():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        autocommit=True,
    )

def exec_sql(cursor, sql, params=None, silent=False):
    if not silent:
        print(f"SQL> {sql if len(sql) < 300 else sql[:300]+' ...'}")
    cursor.execute(sql, params or ())

def fetchall(cursor, sql, params=None):
    cursor.execute(sql, params or ())
    return cursor.fetchall()

def show_server_defaults(cursor):
    rows = fetchall(cursor, "SHOW VARIABLES LIKE 'character_set_server'")
    charset = rows[0][1] if rows else "?"
    rows = fetchall(cursor, "SHOW VARIABLES LIKE 'collation_server'")
    coll = rows[0][1] if rows else "?"
    print(f"[SERVER DEFAULTS] character_set_server={charset}, collation_server={coll}")

def relax_sql_mode(cursor):
    # Показати та послабити sql_mode на час ALTER’ів (прибрати нульові дати обмеження)
    rows = fetchall(cursor, "SELECT @@SESSION.sql_mode")
    old_mode = rows[0][0] if rows and rows[0] else ""
    print(f"[SQL_MODE] session before: {old_mode}")
    exec_sql(cursor, """
        SET SESSION sql_mode = REPLACE(
            REPLACE(@@SESSION.sql_mode,'NO_ZERO_IN_DATE',''),
            'NO_ZERO_DATE',''
        )
    """, silent=True)
    rows = fetchall(cursor, "SELECT @@SESSION.sql_mode")
    new_mode = rows[0][0] if rows and rows[0] else ""
    print(f"[SQL_MODE] session after : {new_mode}")
    return old_mode

def restore_sql_mode(cursor, old_mode):
    esc = (old_mode or "").replace("'", "''")
    exec_sql(cursor, f"SET SESSION sql_mode = '{esc}'", silent=True)
    print("[SQL_MODE] session restored")

def alter_database_default(cursor):
    row = fetchall(cursor, """
        SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
        FROM information_schema.SCHEMATA
        WHERE SCHEMA_NAME = %s
    """, (DB_DATABASE,))
    if not row:
        print(f"[ERROR] БД `{DB_DATABASE}` не знайдена")
        sys.exit(1)

    db_charset, db_collation = row[0]
    print(f"\n[DB] Поточні дефолти: charset={db_charset}, collation={db_collation}")

    if db_collation == TARGET_COLLATION and db_charset == TARGET_CHARSET:
        print("[DB] Дефолти вже ок: зміна не потрібна.")
        return

    print(f"[DB] Змінюю дефолти БД `{DB_DATABASE}` → {TARGET_CHARSET}/{TARGET_COLLATION} ...")
    t0 = time.time()
    exec_sql(cursor, f"""
        ALTER DATABASE `{DB_DATABASE}`
        CHARACTER SET {TARGET_CHARSET}
        COLLATE {TARGET_COLLATION}
    """)
    print(f"[DB] ✅ Готово за {time.time()-t0:.2f}s")

def convert_table(cursor, table_schema: str, table_name: str, current_collation: str):
    print(f"\n🔎 Перевіряю таблицю `{table_schema}.{table_name}` ...")
    print(f"   фактична коляція таблиці: {current_collation}")
    if current_collation != TARGET_COLLATION:
        print(f"   змінюю коляцію таблиці на: {TARGET_COLLATION} ...")
        t0 = time.time()
        exec_sql(cursor, f"""
            ALTER TABLE `{table_schema}`.`{table_name}`
            CONVERT TO CHARACTER SET {TARGET_CHARSET} COLLATE {TARGET_COLLATION}
        """)
        print(f"   ✅ Таблицю змінено за {time.time()-t0:.2f}s")
    else:
        print("   ✅ Вже в потрібній коляції.")

def columns_to_fix(cursor, table_schema: str, table_name: str):
    """Отримати текстові колонки, де колація != TARGET_COLLATION."""
    rows = fetchall(cursor, """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_SET_NAME,
            COLLATION_NAME,
            COLUMN_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            EXTRA
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND CHARACTER_SET_NAME IS NOT NULL
          AND COLLATION_NAME <> %s
    """, (table_schema, table_name, TARGET_COLLATION))
    rows = [r for r in rows if (r[1] or '').lower() in TEXT_DATA_TYPES]
    return rows

def build_modify_clause(col):
    """
    col: (COLUMN_NAME, DATA_TYPE, CHARACTER_SET_NAME, COLLATION_NAME,
          COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA)
    """
    name, data_type, chs, coll, column_type, is_nullable, default, extra = col
    clause = f"MODIFY `{name}` {column_type} CHARACTER SET {TARGET_CHARSET} COLLATE {TARGET_COLLATION}"
    clause += " NOT NULL" if is_nullable == "NO" else " NULL"

    if default is not None:
        if isinstance(default, str) and default.upper() == "NULL":
            clause += " DEFAULT NULL"
        else:
            val = str(default).replace("'", "''")
            clause += f" DEFAULT '{val}'"

    if extra:
        clause += f" {extra}"
    return clause

def fix_columns_in_table(cursor, table_schema: str, table_name: str):
    cols = columns_to_fix(cursor, table_schema, table_name)
    if not cols:
        print(f"   Колонки: ✅ всі в потрібній коляції.")
        return

    print(f"   Колонки: знайдено {len(cols)} з іншою коляцією → перетворюю ...")
    BATCH = 50
    for i in range(0, len(cols), BATCH):
        batch = cols[i:i+BATCH]
        modifies = []
        for col in batch:
            clause = build_modify_clause(col)
            modifies.append(clause)
            colname, _, _, old_coll, *_ = col
            print(f"     🔧 {table_name}.{colname}: {old_coll} → {TARGET_COLLATION}")

        sql = f"ALTER TABLE `{table_schema}`.`{table_name}`\n  " + ",\n  ".join(modifies) + ";"
        t0 = time.time()
        exec_sql(cursor, sql)
        print(f"     ✅ Батч {i+1}-{i+len(batch)} змінено за {time.time()-t0:.2f}s")

def main():
    print(f"[INFO] Підключаюсь до MySQL {DB_HOST}:{DB_PORT}, БД={DB_DATABASE}")
    conn = connect()
    cursor = conn.cursor()

    old_mode = None
    try:
        show_server_defaults(cursor)
        old_mode = relax_sql_mode(cursor)  # ⬅️ критично для '0000-00-00'
        alter_database_default(cursor)

        # 1) Всі таблиці та їх колації
        tables = fetchall(cursor, """
            SELECT TABLE_NAME, TABLE_COLLATION
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
        """, (DB_DATABASE,))

        total = len(tables)
        print(f"\n[INFO] Знайдено {total} таблиць у `{DB_DATABASE}`.")

        # 2) Пройтись по таблицях
        for idx, (table_name, table_collation) in enumerate(tables, start=1):
            print(f"\n[{idx}/{total}] Таблиця `{table_name}`")
            convert_table(cursor, DB_DATABASE, table_name, table_collation)
            fix_columns_in_table(cursor, DB_DATABASE, table_name)

        # 3) Контрольний прохід
        print("\n[CHECK] Контрольний прохід по колонкам з невірною коляцією ...")
        remaining = fetchall(cursor, """
            SELECT TABLE_NAME, COLUMN_NAME, COLLATION_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND CHARACTER_SET_NAME IS NOT NULL
              AND COLLATION_NAME <> %s
        """, (DB_DATABASE, TARGET_COLLATION))

        if not remaining:
            print("✅ Все готово: усі текстові колонки в `utf8mb4_0900_ai_ci`.")
        else:
            print(f"⚠️ Залишились {len(remaining)} колонок з іншою коляцією (див. нижче).")
            for t, c, col in remaining[:50]:
                print(f"   - {t}.{c}: {col}")
            print("   (За потреби запусти скрипт ще раз або виправ точково.)")

    finally:
        try:
            if old_mode is not None:
                restore_sql_mode(cursor, old_mode)
        except Exception:
            pass
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
