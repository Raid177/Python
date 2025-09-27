from aiogram import Router, F
from aiogram.types import Message
from aiogram import Bot
from core.config import settings
from core.db import get_conn
from core.repositories import messages as repo_m
from bot.keyboards.common import prefix_for_staff

router = Router()

@router.message(F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def outbound_to_client(message: Message, bot: Bot):
    conn = get_conn(); 
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM pp_tickets WHERE thread_id=%s ORDER BY id DESC LIMIT 1",
                    (message.message_thread_id,))
        t = cur.fetchone()
    finally:
        conn.close()
    if not t:
        return

    prefix = prefix_for_staff(message.from_user.id)

    if message.content_type == "text":
        out = await bot.send_message(chat_id=t["client_user_id"], text=f"{prefix}\n\n{message.text}")
        conn = get_conn()
        try:
            repo_m.insert(conn, t["id"], "out", out.message_id, message.text, "text")
        finally:
            conn.close()
    else:
        await bot.send_message(chat_id=t["client_user_id"], text=prefix)
        out = await message.copy_to(chat_id=t["client_user_id"])
        conn = get_conn()
        try:
            repo_m.insert(conn, t["id"], "out", out.message_id, getattr(message, "caption", None), message.content_type)
        finally:
            conn.close()
