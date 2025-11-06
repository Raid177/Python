# core/repositories/tickets.py
from typing import Optional, Sequence
from datetime import datetime

def find_open_by_client(conn, client_id: int):
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """SELECT * FROM pp_tickets
                   WHERE client_user_id=%s AND status IN ('open','in_progress')
                   ORDER BY id DESC LIMIT 1""",
        (client_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row


def find_latest_by_client(conn, client_id: int):
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """SELECT * FROM pp_tickets
                   WHERE client_user_id=%s
                   ORDER BY id DESC LIMIT 1""",
        (client_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row


def ensure_latest_by_client(conn, client_id: int):
    return find_latest_by_client(conn, client_id)


def find_by_thread(conn, thread_id: int):
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """SELECT * FROM pp_tickets
                   WHERE thread_id=%s
                   ORDER BY id DESC LIMIT 1""",
        (thread_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row


def get_by_id(conn, ticket_id: int):
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pp_tickets WHERE id=%s LIMIT 1", (ticket_id,))
    row = cur.fetchone()
    cur.close()
    return row


def update_thread(conn, ticket_id: int, thread_id: int):
    cur = conn.cursor()
    cur.execute(
        "UPDATE pp_tickets SET thread_id=%s WHERE id=%s", (thread_id, ticket_id)
    )
    cur.close()


def set_label(conn, ticket_id: int, label: str | None):
    cur = conn.cursor()
    # оновлюємо мітку тікета
    cur.execute(
        "UPDATE pp_tickets SET label=%s WHERE id=%s",
        (label, ticket_id),
    )
    # продублюємо в картку клієнта (JOIN по ІД клієнта та telegram_id)
    cur.execute(
        """
        UPDATE pp_clients AS c
        JOIN pp_tickets AS t
          ON t.id = %s
         AND t.client_user_id = c.telegram_id
        SET c.label = %s,
            c.updated_at = UTC_TIMESTAMP()
        """,
        (ticket_id, label),
    )
    cur.close()


def create(conn, client_id: int, thread_id: int = None):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO pp_tickets (client_user_id, thread_id) VALUES (%s,%s)",
        (client_id, thread_id),
    )
    tid = cur.lastrowid
    cur.close()
    return tid


def set_status(conn, ticket_id: int, status: str):
    cur = conn.cursor()
    cur.execute("UPDATE pp_tickets SET status=%s WHERE id=%s", (status, ticket_id))
    cur.close()


def assign_to(conn, ticket_id: int, telegram_id: int | None):
    cur = conn.cursor()
    cur.execute(
        "UPDATE pp_tickets SET assigned_to=%s WHERE id=%s", (telegram_id, ticket_id)
    )
    cur.close()


def close_ticket(conn, ticket_id: int):
    cur = conn.cursor()

    # 1) закриваємо тікет
    cur.execute(
        "UPDATE pp_tickets SET status='closed', closed_at=UTC_TIMESTAMP() WHERE id=%s",
        (ticket_id,),
    )

    # 2) інкрементуємо лічильник закритих звернень у pp_clients
    #    ЗВЕРНИ УВАГУ: тут використовується c.telegram_id  (НЕ telegram_user_id)
    cur.execute(
        """
        UPDATE pp_clients AS c
        JOIN pp_tickets AS t
          ON t.id=%s
         AND t.client_user_id = c.telegram_id
        SET c.total_closed = COALESCE(c.total_closed, 0) + 1
        """,
        (ticket_id,),
    )

    cur.close()


def reopen(conn, ticket_id: int):
    cur = conn.cursor()
    cur.execute(
        "UPDATE pp_tickets SET status='open', closed_at=NULL, assigned_to=NULL WHERE id=%s",
        (ticket_id,),
    )
    cur.close()


def touch_client(conn, ticket_id: int):
    cur = conn.cursor()
    cur.execute(
        "UPDATE pp_tickets SET last_client_msg_at = UTC_TIMESTAMP() WHERE id=%s",
        (ticket_id,),
    )
    cur.close()


def touch_staff(conn, ticket_id: int):
    cur = conn.cursor()
    cur.execute(
        "UPDATE pp_tickets SET last_staff_msg_at = UTC_TIMESTAMP() WHERE id=%s",
        (ticket_id,),
    )
    cur.close()


def mark_reminded(conn, ticket_id: int):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pp_tickets SET last_reminder_at = UTC_TIMESTAMP() WHERE id = %s",
            (ticket_id,),
        )
    conn.commit()


def mark_unassigned_alerted(conn, ticket_id: int):
    cur = conn.cursor()
    cur.execute(
        "UPDATE pp_tickets SET last_unassigned_alert_at=UTC_TIMESTAMP() WHERE id=%s",
        (ticket_id,),
    )
    cur.close()


def find_idle(conn, min_idle_minutes: int):
    """
    Клієнт чекає відповіді, якщо:
      - тикет open/in_progress
      - assigned_to не NULL
      - останнє повідомлення від клієнта є (last_client_msg_at NOT NULL)
      - остання відповідь співробітника старіша (last_staff_msg_at IS NULL або < last_client_msg_at)
      - минуло >= min_idle_minutes від last_client_msg_at
      - snooze_until не активний
      - і не пінгували надто нещодавно (since_last_reminder >= REMINDER_PING_EVERY_MIN — це ми перевіримо вище в сервісі)
    Повертаємо також зручні метрики.
    """
    q = """
        SELECT
            t.*,
            TIMESTAMPDIFF(MINUTE, t.last_client_msg_at, UTC_TIMESTAMP()) AS idle_minutes,
            CASE
              WHEN t.last_reminder_at IS NULL THEN 99999
              ELSE TIMESTAMPDIFF(MINUTE, t.last_reminder_at, UTC_TIMESTAMP())
            END AS since_last_reminder
        FROM pp_tickets t
        WHERE t.status IN ('open','in_progress')
          AND t.assigned_to IS NOT NULL
          AND t.last_client_msg_at IS NOT NULL
          AND (t.last_staff_msg_at IS NULL OR t.last_client_msg_at > t.last_staff_msg_at)
          AND TIMESTAMPDIFF(MINUTE, t.last_client_msg_at, UTC_TIMESTAMP()) >= %s
          AND (t.snooze_until IS NULL OR t.snooze_until < UTC_TIMESTAMP())
        ORDER BY t.last_client_msg_at ASC
    """
    with conn.cursor(dictionary=True) as cur:
        cur.execute(q, (min_idle_minutes,))
        return cur.fetchall()


def find_unassigned_idle(conn, min_idle_minutes: int):
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          id, client_user_id, label, thread_id, last_client_msg_at, last_unassigned_alert_at,
          TIMESTAMPDIFF(MINUTE, last_client_msg_at, UTC_TIMESTAMP()) AS idle_minutes,
          TIMESTAMPDIFF(MINUTE, IFNULL(last_unassigned_alert_at,'1970-01-01'), UTC_TIMESTAMP()) AS since_last_alert
        FROM pp_tickets
        WHERE status IN ('open','in_progress')
          AND assigned_to IS NULL
          AND last_client_msg_at IS NOT NULL
          AND (last_staff_msg_at IS NULL OR last_staff_msg_at < last_client_msg_at)
          AND TIMESTAMPDIFF(MINUTE, last_client_msg_at, UTC_TIMESTAMP()) >= %s
        ORDER BY last_client_msg_at ASC
        """,
        (min_idle_minutes,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


from datetime import datetime


def set_snooze_until(conn, ticket_id: int, until_dt: datetime | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pp_tickets SET snooze_until=%s, updated_at=UTC_TIMESTAMP() WHERE id=%s",
            (until_dt, ticket_id),
        )


def clear_snooze(conn, ticket_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pp_tickets SET snooze_until=NULL, updated_at=UTC_TIMESTAMP() WHERE id=%s",
            (ticket_id,),
        )


def get_by_thread(conn, thread_id: int) -> dict | None:
    with conn.cursor(dictionary=True) as cur:
        cur.execute("SELECT * FROM pp_tickets WHERE thread_id=%s LIMIT 1", (thread_id,))
        return cur.fetchone()

# додай у кінець файлу
def list_ids_by_client(conn, client_user_id: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM pp_tickets WHERE client_user_id=%s", (client_user_id,))
        return [row[0] for row in cur.fetchall()]

def count_messages_by_ticket_ids(conn, ticket_ids: list[int]) -> int:
    if not ticket_ids:
        return 0
    with conn.cursor() as cur:
        q = "SELECT COUNT(*) FROM pp_messages WHERE ticket_id IN (" + ",".join(["%s"] * len(ticket_ids)) + ")"
        cur.execute(q, ticket_ids)
        return int(cur.fetchone()[0])

def delete_messages_by_ticket_ids(conn, ticket_ids: list[int]) -> int:
    if not ticket_ids:
        return 0
    with conn.cursor() as cur:
        q = "DELETE FROM pp_messages WHERE ticket_id IN (" + ",".join(["%s"] * len(ticket_ids)) + ")"
        cur.execute(q, ticket_ids)
        return cur.rowcount

def count_tickets_by_client(conn, client_user_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pp_tickets WHERE client_user_id=%s", (client_user_id,))
        return int(cur.fetchone()[0])

def delete_tickets_by_client(conn, client_user_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pp_tickets WHERE client_user_id=%s", (client_user_id,))
        return cur.rowcount
