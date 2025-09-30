#core/repositories/agents.py

def get_display_name(conn, telegram_id: int) -> str | None:
    cur = conn.cursor()
    cur.execute(
        """SELECT display_name FROM pp_agents
           WHERE telegram_id=%s AND active=1""",
        (telegram_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def upsert_agent(conn, telegram_id: int, display_name: str, role: str = "doctor", active: int = 1):
    """
    Додати співробітника або оновити його відображуване ім'я.
    """
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO pp_agents (telegram_id, display_name, role, active)
           VALUES (%s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
             display_name = VALUES(display_name),
             role = VALUES(role),
             active = VALUES(active)""",
        (telegram_id, display_name, role, active),
    )
    cur.close()


def set_display_name(conn, telegram_id: int, display_name: str, activate: bool = True):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pp_agents (telegram_id, display_name, role, active)
            VALUES (%s, %s,
                    COALESCE((SELECT role FROM pp_agents WHERE telegram_id=%s), 'doctor'),
                    %s)
            ON DUPLICATE KEY UPDATE
                display_name = VALUES(display_name),
                active = CASE WHEN %s=1 THEN 1 ELSE active END
        """, (telegram_id, display_name, telegram_id, 1 if activate else 0, 1 if activate else 0))

def list_active(conn):
    """
    Список активних співробітників для побудови меню призначення.
    Повертає масив словників: {telegram_id, display_name}
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """SELECT telegram_id, display_name
           FROM pp_agents
           WHERE active=1
           ORDER BY display_name ASC, telegram_id ASC"""
    )
    rows = cur.fetchall()
    cur.close()
    return rows

def get_agent(conn, telegram_id: int) -> dict | None:
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """SELECT display_name, role, active
           FROM pp_agents
           WHERE telegram_id=%s AND active=1""",
        (telegram_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row
