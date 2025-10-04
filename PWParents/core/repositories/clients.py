# core/repositories/clients.py

from typing import Optional

def get_client(conn, telegram_id: int) -> Optional[dict]:
    """
    Повертаємо всі поля, які читає код (в т.ч. last_phone_prompt_at і phone_confirmed),
    щоб логіка підказки працювала коректно.
    """
    with conn.cursor(dictionary=True) as cur:
        cur.execute(
            """
            SELECT
                telegram_id,
                phone,
                phone_confirmed,
                label,
                total_closed,
                consent_ts,
                created_at,
                updated_at,
                last_phone_prompt_at
            FROM pp_clients
            WHERE telegram_id = %s
            LIMIT 1
            """,
            (telegram_id,),
        )
        return cur.fetchone()


def ensure_exists(conn, telegram_id: int):
    """
    Гарантує наявність запису для клієнта.
    НІЧОГО зайвого не перетираємо.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pp_clients (telegram_id, created_at, updated_at)
            VALUES (%s, UTC_TIMESTAMP(), UTC_TIMESTAMP())
            ON DUPLICATE KEY UPDATE telegram_id = VALUES(telegram_id)
            """,
            (telegram_id,),
        )
    # commit ззовні або тут — залежно від твоєї політики. Залишу ззовні, як було.


def upsert_client(
    conn,
    telegram_id: int,
    phone: str | None,
    phone_confirmed: bool | None = None,
    gave_consent: bool = False,
    label: str | None = None,
) -> None:
    """
    Створює/оновлює клієнта без зайвих перетирань.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pp_clients (
                telegram_id, phone, phone_confirmed, label, consent_ts,
                created_at, updated_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                CASE WHEN %s THEN UTC_TIMESTAMP() ELSE NULL END,
                UTC_TIMESTAMP(),
                UTC_TIMESTAMP()
            )
            ON DUPLICATE KEY UPDATE
                updated_at = UTC_TIMESTAMP(),
                phone = COALESCE(VALUES(phone), phone),
                phone_confirmed = COALESCE(VALUES(phone_confirmed), phone_confirmed),
                label = COALESCE(VALUES(label), label),
                consent_ts = IF(consent_ts IS NULL AND %s, UTC_TIMESTAMP(), consent_ts)
            """,
            (
                telegram_id,
                phone,
                (1 if phone_confirmed is True else (0 if phone_confirmed is False else None)),
                label,
                bool(gave_consent),
                bool(gave_consent),
            ),
        )
    # commit ззовні


def set_label(conn, telegram_id: int, label: str):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pp_clients SET label=%s, updated_at=UTC_TIMESTAMP() WHERE telegram_id=%s",
            (label, telegram_id),
        )


def inc_closed(conn, telegram_id: int):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pp_clients SET total_closed = total_closed + 1, updated_at=UTC_TIMESTAMP() WHERE telegram_id=%s",
            (telegram_id,),
        )


def mark_phone_prompted(conn, telegram_id: int):
    """
    Фіксуємо момент показу підказки. ВАЖЛИВО: фільтр за telegram_id.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pp_clients SET last_phone_prompt_at = UTC_TIMESTAMP() WHERE telegram_id = %s",
            (telegram_id,),
        )
