# bot/service/msglog.py
from core.db import get_conn
from core.repositories import messages as repo_m
from core.repositories import tickets as repo_t

def log_and_touch(ticket_id: int, direction: str, tg_message_id: int, text: str | None, content_type: str):
    """
    direction: "in"  (від клієнта)  -> touch_client
               "out" (від саппорта) -> touch_staff
    """
    conn = get_conn()
    try:
        repo_m.insert(conn, ticket_id, direction, tg_message_id, text, content_type)
        if direction == "in":
            repo_t.touch_client(conn, ticket_id)
        elif direction == "out":
            repo_t.touch_staff(conn, ticket_id)
    finally:
        conn.close()
