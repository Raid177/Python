# app/main.py
from __future__ import annotations
import logging
import sys
import signal
import traceback
import os
import subprocess
import mysql.connector

from app.env_loader import load_settings
from app.logging_setup import setup_logging

# push
try:
    from app.enote_push import push_to_enote  # читає DRY_RUN і ENOTE_PUSH_LIMIT з .env
except Exception:
    push_to_enote = None  # type: ignore

log = logging.getLogger("main")


def _setup_signals():
    def _handler(signum, frame):
        log.warning("⛔ Отримано сигнал %s, завершення...", signum)
        sys.exit(130)
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name, "")
    return v if v != "" else default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (_env(name).strip().lower())
    if raw == "":
        return default
    return raw in ("1", "true", "yes", "on")


def _env_int_pos(name: str) -> int | None:
    raw = _env(name).strip()
    if not raw:
        return None
    try:
        n = int(raw)
        return n if n > 0 else None
    except Exception:
        return None


# STEP 1: fetch PrivatBank
def fetch_privat_transactions():
    try:
        from app import fetch_bank_data  # type: ignore
        log.info("🏦 Отримую транзакції з Привату (import app.fetch_bank_data)...")
        if hasattr(fetch_bank_data, "main"):
            fetch_bank_data.main()  # type: ignore
        elif hasattr(fetch_bank_data, "fetch_bank_data"):
            fetch_bank_data.fetch_bank_data()  # type: ignore
        else:
            raise RuntimeError("app.fetch_bank_data: немає ні main(), ні fetch_bank_data()")
        log.info("✅ Отримано транзакції (через import).")
        return
    except Exception as e:
        log.warning("Не вдалось через import: %s. Пробую subprocess -m ...", e)

    cmd = [sys.executable, "-m", "app.fetch_bank_data"]
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../BankToEnote
    env = os.environ.copy()
    env["PYTHONPATH"] = project_root + (os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else "")
    log.info("🏦 Отримую транзакції з Привату (subprocess: %s)...", " ".join(cmd))
    res = subprocess.run(cmd, capture_output=True, text=True, cwd=project_root, env=env)
    if res.returncode != 0:
        log.error("Помилка при отриманні транзакцій:\nSTDOUT:\n%s\nSTDERR:\n%s", res.stdout, res.stderr)
        raise SystemExit(1)
    log.info("✅ Отримано транзакції (через subprocess -m).\n%s", res.stdout.strip()[-500:])


# STEP 2: commission table
def build_commission_table():
    s = load_settings()
    conn = mysql.connector.connect(
        host=s.db_host, port=s.db_port, user=s.db_user, password=s.db_password, database=s.db_database
    )
    cur = conn.cursor()

    def column_exists(db: str, table: str, col: str) -> bool:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND LOWER(column_name)=LOWER(%s)
            LIMIT 1
        """, (db, table, col))
        return cur.fetchone() is not None

    base_table = "bnk_trazact_prvt_ekv"
    ext_table  = "bnk_trazact_prvt_ekv_ext"

    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s LIMIT 1
    """, (s.db_database, base_table))
    if cur.fetchone() is None:
        log.warning("⚠️  Таблиця %s.%s не знайдена — пропускаю крок комісій.", s.db_database, base_table)
        cur.close(); conn.close(); return

    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s LIMIT 1
    """, (s.db_database, ext_table))
    if cur.fetchone() is None:
        cur.execute(f"CREATE TABLE `{ext_table}` LIKE `{base_table}`")

    if not column_exists(s.db_database, ext_table, "bank_fee"):
        cur.execute(f"ALTER TABLE `{ext_table}` ADD COLUMN `bank_fee` DOUBLE NOT NULL DEFAULT 0")
    if not column_exists(s.db_database, ext_table, "amount_with_fee"):
        cur.execute(f"ALTER TABLE `{ext_table}` ADD COLUMN `amount_with_fee` DOUBLE NULL")

    cur.execute("""
        SELECT LOWER(column_name)
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
    """, (s.db_database, base_table))
    cols = {r[0] for r in cur.fetchall()}

    amount_col = None
    for cand in ("amount", "сумма", "summa", "amount_uah"):
        if cand in cols:
            amount_col = cand
            break
    if amount_col is None:
        log.error("❌ Не знайдено колонку суми (amount/Сумма/...) у %s.%s — пропускаю крок комісій.",
                  s.db_database, base_table)
        cur.close(); conn.close(); return

    fee_col = None
    for cand in ("bank_fee", "fee", "commission", "comis", "комиссия"):
        if cand in cols:
            fee_col = cand
            break
    fee_expr = f"`{fee_col}`" if fee_col else "0"

    cur.execute(f"TRUNCATE `{ext_table}`")
    insert_sql = f"""
        INSERT INTO `{ext_table}`
        SELECT
            t.*,
            {fee_expr} AS bank_fee,
            CASE
                WHEN `{amount_col}` < 0 THEN `{amount_col}` - ABS({fee_expr})
                ELSE `{amount_col}`
            END AS amount_with_fee
        FROM `{base_table}` t
    """
    cur.execute(insert_sql)
    conn.commit()

    log.info("✅ Commission table rebuilt: %s.%s (amount=%s, fee=%s)",
             s.db_database, ext_table, amount_col, (fee_col or "0 (not found)"))

    cur.close(); conn.close()


# STEP 3: counterparties refresh
def refresh_counterparties():
    path = _env("PATH_ENOTE_COUNTERPARTY_SCRIPT")
    if not _env_bool("COUNTERPARTY_REFRESH_BEFORE_RUN", True):
        log.info("🔄 Оновлення контрагентів вимкнено прапором.")
        return
    if not path or not os.path.isfile(path):
        log.warning("⚠️  Скрипт оновлення контрагентів не знайдено — пропускаю.")
        return
    log.info("🔄 Оновлюю контрагентів (%s)...", os.path.basename(path))
    res = subprocess.run([sys.executable, path], capture_output=True, text=True)
    if res.returncode != 0:
        log.error("Помилка при оновленні контрагентів:\n%s\n%s", res.stdout, res.stderr)
        raise SystemExit(1)
    log.info("✅ Контрагенти оновлені.")


# STEP 4: push to Enote
def push_to_enote_wrapper():
    if push_to_enote is None:
        log.error("push_to_enote не імпортується. Перевір app/enote_push.py")
        raise SystemExit(2)
    log.info("📤 Створюю чеки в Єноті...")
    # параметри читає з .env усередині
    push_to_enote()
    log.info("✅ Чеки створено.")


def main() -> int:
    _setup_signals()
    s = load_settings()
    setup_logging(level=s.log_level, log_file=s.log_file)

    dry = (_env("DRY_RUN", "0").strip().lower() not in ("", "0", "false", "no", "off"))
    lim = _env_int_pos("ENOTE_PUSH_LIMIT")
    lim_text = lim if lim is not None else "∞"

    log.info("🚀 Запуск повного циклу Bank→Enote")
    log.info("ENV=%s | DRY_RUN=%s | LIMIT=%s", getattr(s, "enote_env", "?"), dry, lim_text)

    try:
        fetch_privat_transactions()
        build_commission_table()
        refresh_counterparties()
        push_to_enote_wrapper()
        log.info("🏁 Успішне завершення повного циклу.")
        return 0
    except KeyboardInterrupt:
        log.warning("Перервано користувачем.")
        return 130
    except Exception:
        log.error("❌ Помилка:\n%s", traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
