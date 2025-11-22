def insert(
    conn,
    ticket_id: int,
    direction: str,
    tg_msg_id: int,
    text: str = None,
    media_type: str = None,
):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO pp_messages (ticket_id, direction, tg_msg_id, text, media_type)
                   VALUES (%s,%s,%s,%s,%s)""",
        (ticket_id, direction, tg_msg_id, text, media_type),
    )
    cur.close()
