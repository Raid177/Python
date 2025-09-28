# core/repositories/clients.py
def upsert_client(conn, telegram_id: int, phone: str | None, consent: bool):
    cur = conn.cursor()
    if consent and phone:
        cur.execute(
            """INSERT INTO pp_clients (telegram_id, phone, consent_ts)
               VALUES (%s, %s, NOW())
               ON DUPLICATE KEY UPDATE
                 phone = VALUES(phone),
                 consent_ts = IFNULL(consent_ts, NOW())""",
            (telegram_id, phone),
        )
    else:
        cur.execute(
            """INSERT INTO pp_clients (telegram_id) VALUES (%s)
               ON DUPLICATE KEY UPDATE telegram_id = telegram_id""",
            (telegram_id,),
        )
    cur.close()

def get_client(conn, telegram_id: int):
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pp_clients WHERE telegram_id=%s", (telegram_id,))
    row = cur.fetchone()
    cur.close()
    return row
