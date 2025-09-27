from core.db import get_conn
from core.repositories.agents import get_display_name

def prefix_for_staff(staff_tg_id:int) -> str:
    conn = get_conn()
    try:
        name = get_display_name(conn, staff_tg_id)
    finally:
        conn.close()
    label = name if name else f"ID{staff_tg_id}"
    return f"👩‍⚕️ {label}:"
