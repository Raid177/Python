from aiogram import Bot
from core.db import get_conn
from core.repositories import tickets as repo_t

async def ensure_ticket(bot: Bot, support_group_id:int, client_id:int):
    conn = get_conn()
    try:
        t = repo_t.find_open_by_client(conn, client_id)
        if t and t.get("thread_id"):
            return t

        topic = await bot.create_forum_topic(chat_id=support_group_id, name=f"ID{client_id}")
        msg = await bot.send_message(
            chat_id=support_group_id,
            message_thread_id=topic.message_thread_id,
            text=f"🟢 Нове звернення\nКлієнт: <code>{client_id}</code>\nСтатус: open",
        )
        thread_id = msg.message_thread_id

        if t:
            repo_t.update_thread(conn, t["id"], thread_id)
            return {**t, "thread_id": thread_id}
        else:
            tid = repo_t.create(conn, client_id, thread_id)
            return {"id": tid, "client_user_id": client_id, "thread_id": thread_id, "status": "open"}
    finally:
        conn.close()
