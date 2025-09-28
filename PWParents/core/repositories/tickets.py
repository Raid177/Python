def find_open_by_client(conn, client_id:int):
    cur = conn.cursor(dictionary=True)
    cur.execute("""SELECT * FROM pp_tickets
                   WHERE client_user_id=%s AND status IN ('open','in_progress')
                   ORDER BY id DESC LIMIT 1""", (client_id,))
    row = cur.fetchone(); cur.close()
    return row

def find_latest_by_client(conn, client_id:int):
    cur = conn.cursor(dictionary=True)
    cur.execute("""SELECT * FROM pp_tickets
                   WHERE client_user_id=%s
                   ORDER BY id DESC LIMIT 1""", (client_id,))
    row = cur.fetchone(); cur.close()
    return row

def ensure_latest_by_client(conn, client_id:int):
    return find_latest_by_client(conn, client_id)

def find_by_thread(conn, thread_id:int):
    cur = conn.cursor(dictionary=True)
    cur.execute("""SELECT * FROM pp_tickets
                   WHERE thread_id=%s
                   ORDER BY id DESC LIMIT 1""", (thread_id,))
    row = cur.fetchone(); cur.close()
    return row

def get_by_id(conn, ticket_id:int):
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pp_tickets WHERE id=%s LIMIT 1", (ticket_id,))
    row = cur.fetchone(); cur.close()
    return row

def update_thread(conn, ticket_id:int, thread_id:int):
    cur = conn.cursor()
    cur.execute("UPDATE pp_tickets SET thread_id=%s WHERE id=%s", (thread_id, ticket_id))
    cur.close()

def create(conn, client_id:int, thread_id:int=None):
    cur = conn.cursor()
    cur.execute("INSERT INTO pp_tickets (client_user_id, thread_id) VALUES (%s,%s)",
                (client_id, thread_id))
    tid = cur.lastrowid; cur.close()
    return tid

def set_status(conn, ticket_id:int, status:str):
    cur = conn.cursor()
    cur.execute("UPDATE pp_tickets SET status=%s WHERE id=%s", (status, ticket_id))
    cur.close()

def assign_to(conn, ticket_id:int, telegram_id:int | None):
    cur = conn.cursor()
    cur.execute("UPDATE pp_tickets SET assigned_to=%s WHERE id=%s", (telegram_id, ticket_id))
    cur.close()

def close_ticket(conn, ticket_id:int):
    cur = conn.cursor()
    cur.execute("UPDATE pp_tickets SET status='closed', closed_at=NOW() WHERE id=%s", (ticket_id,))
    cur.close()

def reopen(conn, ticket_id:int):
    cur = conn.cursor()
    cur.execute("UPDATE pp_tickets SET status='open', closed_at=NULL, assigned_to=NULL WHERE id=%s",
                (ticket_id,))
    cur.close()
