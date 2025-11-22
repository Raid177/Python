# core/repositories/cnt_ticket_enote.py

from core.db import get_conn
from typing import Optional

def upsert_ticket_enote(
    ticket_id: int,
    client_user_id: int,
    owner_ref_key: str,
    card_ref_key: str,
    visit_ref_key: Optional[str] = None,
    sync_status: str = "pending",
    error_text: Optional[str] = None
):
    """
    Записуємо або оновлюємо запис у pp_ticket_enote після агрегації переписки.
    """
    query = """
    INSERT INTO pp_ticket_enote
    (ticket_id, client_user_id, owner_ref_key, card_ref_key, visit_ref_key, sync_status, error_text, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    ON DUPLICATE KEY UPDATE
    visit_ref_key = VALUES(visit_ref_key),
    sync_status = VALUES(sync_status),
    error_text = VALUES(error_text),
    updated_at = NOW()
    """

    # Виконуємо SQL-запит з переданими параметрами
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, (ticket_id, client_user_id, owner_ref_key, card_ref_key, visit_ref_key, sync_status, error_text))
        conn.commit()
