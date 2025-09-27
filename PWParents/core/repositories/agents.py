def get_display_name(conn, telegram_id:int) -> str | None:
    cur = conn.cursor()
    cur.execute("""SELECT display_name FROM pp_agents
                   WHERE telegram_id=%s AND active=1""",
                (telegram_id,))
    row = cur.fetchone(); cur.close()
    return row[0] if row else None
