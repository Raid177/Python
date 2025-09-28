from aiogram import Bot
from core.db import get_conn
from core.repositories import tickets as repo_t
from bot.keyboards.common import ticket_actions_kb

async def ensure_ticket(bot: Bot, support_group_id:int, client_id:int):
    conn = get_conn()
    try:
        # шукаємо останній тікет клієнта (будь-якого статусу)
        t = repo_t.ensure_latest_by_client(conn, client_id)

        if t and t.get("thread_id"):
            # якщо був закритий — перевідкриємо
            if t["status"] == "closed":
                repo_t.reopen(conn, t["id"])
                await bot.send_message(
                    chat_id=support_group_id,
                    message_thread_id=t["thread_id"],
                    text=f"🟢 Перевідкрито звернення\nКлієнт: <code>{client_id}</code>\nСтатус: open",
                    reply_markup=ticket_actions_kb(client_id),
                )
            return t

        # ще не було теми — створюємо
        topic = await bot.create_forum_topic(chat_id=support_group_id, name=f"ID{client_id}")
        msg = await bot.send_message(
            chat_id=support_group_id,
            message_thread_id=topic.message_thread_id,
            text=f"🟢 Нове звернення\nКлієнт: <code>{client_id}</code>\nСтатус: open",
            reply_markup=ticket_actions_kb(client_id),
        )
        thread_id = msg.message_thread_id

        if t:
            repo_t.update_thread(conn, t["id"], thread_id)
            return {**t, "thread_id": thread_id, "status": "open"}
        else:
            tid = repo_t.create(conn, client_id, thread_id)
            return {"id": tid, "client_user_id": client_id, "thread_id": thread_id, "status": "open"}
    finally:
        conn.close()
