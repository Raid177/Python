# scripts/reset_client.py
# видалення клієнта з БД (обнулення)


import os
import sys
import pymysql
from dotenv import load_dotenv

# -------------------------------
# Налаштування
# -------------------------------
ENV_PATH = "/root/Python/PWParents/.env"
TELEGRAM_ID_TO_RESET = 6557995963  # ← поміняй тут на потрібний ID
DRY_RUN = False  # ← True = тільки показати, що буде видалено

# -------------------------------
# Завантаження .env
# -------------------------------
if not os.path.exists(ENV_PATH):
    print(f"[ERROR] .env не знайдено: {ENV_PATH}")
    sys.exit(1)

load_dotenv(ENV_PATH)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
    "autocommit": False,
}


def table_exists(cur, table_name: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (table_name,))
    return cur.fetchone() is not None


def reset_client(conn, telegram_id: int):
    cur = conn.cursor()

    # 1) знайдемо тікети
    cur.execute("SELECT id FROM pp_tickets WHERE client_user_id=%s", (telegram_id,))
    ticket_ids = [row[0] for row in cur.fetchall()]
    print(f"Знайдено тікетів клієнта {telegram_id}: {len(ticket_ids)} -> {ticket_ids}")

    # 2) видалимо повідомлення цих тікетів
    if ticket_ids:
        placeholders = ", ".join(["%s"] * len(ticket_ids))
        delete_msgs_sql = f"DELETE FROM pp_messages WHERE ticket_id IN ({placeholders})"
        if DRY_RUN:
            print(f"[DRY] {delete_msgs_sql}  {ticket_ids}")
        else:
            cur.execute(delete_msgs_sql, ticket_ids)
            print(f"[OK] Видалено з pp_messages: {cur.rowcount}")

    # 3) (опціонально) видалимо «висячий намір» з pp_client_intents
    if table_exists(cur, "pp_client_intents"):
        if DRY_RUN:
            print(
                f"[DRY] DELETE FROM pp_client_intents WHERE client_user_id={telegram_id}"
            )
        else:
            cur.execute(
                "DELETE FROM pp_client_intents WHERE client_user_id=%s", (telegram_id,)
            )
            print(f"[OK] Видалено з pp_client_intents: {cur.rowcount}")

    # 4) видалимо тікети
    if DRY_RUN:
        print(f"[DRY] DELETE FROM pp_tickets WHERE client_user_id={telegram_id}")
    else:
        cur.execute("DELETE FROM pp_tickets WHERE client_user_id=%s", (telegram_id,))
        print(f"[OK] Видалено з pp_tickets: {cur.rowcount}")

    # 5) видалимо клієнта
    if DRY_RUN:
        print(f"[DRY] DELETE FROM pp_clients WHERE telegram_id={telegram_id}")
    else:
        cur.execute("DELETE FROM pp_clients WHERE telegram_id=%s", (telegram_id,))
        print(f"[OK] Видалено з pp_clients: {cur.rowcount}")

    cur.close()


def main():
    print(f"🔹 Підключення до {os.getenv('DB_NAME')} ...")
    conn = pymysql.connect(**DB_CONFIG)
    try:
        reset_client(conn, TELEGRAM_ID_TO_RESET)
        if DRY_RUN:
            conn.rollback()
            print("✅ DRY-RUN завершено. Зміни НЕ збережено.")
        else:
            conn.commit()
            print("✅ Видалення завершено. Транзакцію зафіксовано.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
