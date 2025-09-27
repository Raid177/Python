def find_open_by_client(conn, client_id:int):
    cur = conn.cursor(dictionary=True)
    cur.execute("""SELECT * FROM pp_tickets
                   WHERE client_user_id=%s AND status IN ('open','in_progress')
                   ORDER BY id DESC LIMIT 1""",(client_id,))
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
