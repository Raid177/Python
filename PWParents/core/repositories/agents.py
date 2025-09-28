def get_display_name(conn, telegram_id:int) -> str | None:
    cur = conn.cursor()
    cur.execute("""SELECT display_name FROM pp_agents
                   WHERE telegram_id=%s AND active=1""",
                (telegram_id,))
    row = cur.fetchone(); cur.close()
    return row[0] if row else None

def list_active(conn):
    cur = conn.cursor(dictionary=True)
    cur.execute("""SELECT telegram_id, display_name
                   FROM pp_agents
                   WHERE active=1
                   ORDER BY display_name ASC""")
    rows = cur.fetchall()
    cur.close()
    return rows
