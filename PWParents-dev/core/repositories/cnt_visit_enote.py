# core/repositories/cnt_visit_enote.py

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from core.db import get_conn


@dataclass
class TicketEnoteRow:
    ticket_id: int
    card_ref_key: Optional[str]
    visit_ref_key: Optional[str]
    last_run_at: Optional[datetime]
    status: str
    last_error: Optional[str]


def get_by_ticket_id(ticket_id: int) -> Optional[TicketEnoteRow]:
    """
    Повертає запис з pp_ticket_enote по ticket_id, або None якщо його немає.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticket_id, card_ref_key, visit_ref_key, last_run_at, status, last_error
                FROM pp_ticket_enote
                WHERE ticket_id = %s
                """,
                (ticket_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return None

    return TicketEnoteRow(
        ticket_id=row[0],
        card_ref_key=row[1],
        visit_ref_key=row[2],
        last_run_at=row[3],
        status=row[4],
        last_error=row[5],
    )


def upsert_ticket_enote(
    ticket_id: int,
    card_ref_key: Optional[str],
    visit_ref_key: Optional[str],
    status: str,
    last_error: Optional[str] = None,
) -> None:
    """
    Створює або оновлює запис у pp_ticket_enote по ticket_id.
    last_run_at завжди ставимо NOW().
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pp_ticket_enote (ticket_id, card_ref_key, visit_ref_key, last_run_at, status, last_error)
                VALUES (%s, %s, %s, NOW(), %s, %s)
                ON DUPLICATE KEY UPDATE
                    card_ref_key = VALUES(card_ref_key),
                    visit_ref_key = VALUES(visit_ref_key),
                    last_run_at = VALUES(last_run_at),
                    status = VALUES(status),
                    last_error = VALUES(last_error)
                """,
                (ticket_id, card_ref_key, visit_ref_key, status, last_error),
            )
        conn.commit()
    finally:
        conn.close()
