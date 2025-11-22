# core/repositories/cnt_chat_repo.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

from core.db import get_conn


# -----------------------------
#   DATA CLASSES (DTO)
# -----------------------------

@dataclass
class Ticket:
    id: int
    client_user_id: int
    status: str
    created_at: datetime
    closed_at: Optional[datetime]


@dataclass
class Client:
    telegram_id: int
    owner_ref_key: Optional[str]
    label: Optional[str]


@dataclass
class MessageRow:
    """
    Мінімальний набір полів, який потрібен для GPT-агрегації.
    """
    id: int
    ticket_id: int
    direction: str       # 'in' / 'out'
    text: str
    created_at: datetime


# ------------------------------------------------------
#   REPOSITORY FUNCTIONS
# ------------------------------------------------------

def get_ticket_with_client(ticket_id: int) -> tuple[Ticket, Client]:
    """
    Дістає тікет із pp_tickets + клієнта з pp_clients.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, client_user_id, status, created_at, closed_at
                FROM pp_tickets
                WHERE id=%s
                """,
                (ticket_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Ticket {ticket_id} not found")

            ticket = Ticket(*row)

            cur.execute(
                """
                SELECT telegram_id, owner_ref_key, label
                FROM pp_clients
                WHERE telegram_id=%s
                """,
                (ticket.client_user_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Client {ticket.client_user_id} not found")

            client = Client(*row)

        return ticket, client
    finally:
        conn.close()


def get_text_messages_for_ticket_after(
    ticket_id: int,
    last_msg_id: Optional[int],
) -> List[MessageRow]:
    """
    Тягне текстові повідомлення по тікету з pp_messages,
    де media_type='text', text IS NOT NULL,
    і id > last_msg_id (якщо він є).
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if last_msg_id:
                cur.execute(
                    """
                    SELECT id, ticket_id, direction, text, created_at
                    FROM pp_messages
                    WHERE ticket_id=%s
                      AND media_type='text'
                      AND text IS NOT NULL
                      AND id > %s
                    ORDER BY id ASC
                    """,
                    (ticket_id, last_msg_id),
                )
            else:
                cur.execute(
                    """
                    SELECT id, ticket_id, direction, text, created_at
                    FROM pp_messages
                    WHERE ticket_id=%s
                      AND media_type='text'
                      AND text IS NOT NULL
                    ORDER BY id ASC
                    """,
                    (ticket_id,),
                )

            rows = cur.fetchall()

        return [MessageRow(*row) for row in rows]
    finally:
        conn.close()


def get_ticket_messages(ticket_id: int) -> List[MessageRow]:
    """
    Повертає список текстових повідомлень по тікету
    у вигляді MessageRow.

    Вибираємо:
      - тільки media_type = 'text'
      - text IS NOT NULL і не порожній
    Сортуємо за часом створення (created_at, id).
    """
    sql = """
        SELECT
            id,
            ticket_id,
            direction,
            text,
            created_at
        FROM pp_messages
        WHERE ticket_id = %s
          AND media_type = 'text'
          AND text IS NOT NULL
          AND text <> ''
        ORDER BY created_at ASC, id ASC
    """

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (ticket_id,))
            rows = cur.fetchall()

        return [MessageRow(*row) for row in rows]
    finally:
        conn.close()


def get_ticket_by_thread_id(thread_id: int) -> Optional[Dict[str, Any]]:
    """
    Повертає останній тікет для заданого thread_id (темі в саппорт-групі).
    Використовується для /visit_preview, /visit_* команд.

    Якщо нічого не знайдено – повертає None.
    """
    sql = """
        SELECT
            id,
            client_user_id,
            thread_id,
            label,
            status,
            assigned_to,
            last_client_msg_at,
            last_staff_msg_at,
            created_at,
            closed_at
        FROM pp_tickets
        WHERE thread_id = %s
        ORDER BY id DESC
        LIMIT 1
    """

    conn = get_conn()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(sql, (thread_id,))
            row = cur.fetchone()
            return row if row else None
    finally:
        conn.close()


def get_agent_enote_ref_key(operator_tg_id: int) -> Optional[str]:
    """
    Дістає enote_ref_key з pp_agents для оператора (за telegram_id).
    Залишено для зворотної сумісності зі старим кодом.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT enote_ref_key
                FROM pp_agents
                WHERE telegram_id=%s
                """,
                (operator_tg_id,),
            )
            row = cur.fetchone()

        return row[0] if row else None
    finally:
        conn.close()
