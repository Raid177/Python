# core/repositories/clients.py

from typing import Optional

def get_client(conn, telegram_id: int) -> Optional[dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT telegram_id, phone, label, total_closed, consent_ts, created_at, updated_at "
        "FROM pp_clients WHERE telegram_id=%s LIMIT 1",
        (telegram_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row


def ensure_exists(conn, telegram_id: int):
    """
    Гарантує наявність запису для клієнта.
    Якщо такого telegram_id ще немає — створює порожній рядок.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pp_clients (telegram_id, created_at, updated_at)
        VALUES (%s, UTC_TIMESTAMP(), UTC_TIMESTAMP())
        ON DUPLICATE KEY UPDATE
          updated_at = UTC_TIMESTAMP()
        """,
        (telegram_id,),
    )
    cur.close()


def upsert_client(conn, telegram_id: int, phone: Optional[str], gave_consent: bool, label: Optional[str] = None):
    """
    Оновлює/створює клієнта:
      - якщо передали phone — зберігаємо
      - якщо gave_consent=True — виставляємо consent_ts (один раз)
      - якщо передали label — оновлюємо label
    Інші поля не чіпаємо.
    """
    cur = conn.cursor()

    # будуємо SET динамічно, щоб не перетирати зайвого
    sets = ["updated_at=UTC_TIMESTAMP()"]
    params = []

    if phone is not None:
        sets.append("phone=%s")
        params.append(phone)

    if label is not None:
        sets.append("label=%s")
        params.append(label)

    if gave_consent:
        sets.append("consent_ts=IFNULL(consent_ts, UTC_TIMESTAMP())")

    set_sql = ", ".join(sets)

    # вставка або оновлення
    cur.execute(
        f"""
        INSERT INTO pp_clients (telegram_id, phone, label, consent_ts, created_at, updated_at)
        VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP())
        ON DUPLICATE KEY UPDATE {set_sql}
        """,
        (
            telegram_id,
            phone,
            label,
            # при вставці – одразу ставимо consent_ts, якщо дали згоду, інакше NULL
            (None if not gave_consent else None)  # значення для VALUES(consent_ts) – не критично, оновиться через ON DUP
        ),
    )

    # Якщо це був апдейт без вставки, нам потрібно виконати окремий UPDATE зі
    # зібраним SET (бо в ON DUPLICATE ми вже його виконали). Вище ми все закрили через ON DUPLICATE.
    # Тут нічого додатково робити не треба.

    cur.close()


def set_label(conn, telegram_id: int, label: str):
    cur = conn.cursor()
    cur.execute(
        "UPDATE pp_clients SET label=%s, updated_at=UTC_TIMESTAMP() WHERE telegram_id=%s",
        (label, telegram_id),
    )
    cur.close()


def inc_closed(conn, telegram_id: int):
    cur = conn.cursor()
    cur.execute(
        "UPDATE pp_clients SET total_closed = total_closed + 1, updated_at=UTC_TIMESTAMP() WHERE telegram_id=%s",
        (telegram_id,),
    )
    cur.close()
