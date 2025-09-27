from aiogram import Bot
from core.db import get_conn
from core.repositories import messages as repo_m

async def log_and_send_text_to_topic(bot: Bot, support_group_id:int, thread_id:int, ticket_id:int, text:str, head:str):
    sent = await bot.send_message(chat_id=support_group_id, message_thread_id=thread_id, text=f"{head}\n\n{text}")
    conn = get_conn(); 
    try:
        repo_m.insert(conn, ticket_id, "in", sent.message_id, text, "text")
    finally:
        conn.close()

async def log_inbound_media_copy(message, support_group_id:int, thread_id:int, ticket_id:int, head:str):
    sent = await message.copy_to(chat_id=support_group_id, message_thread_id=thread_id, caption=head)
    conn = get_conn(); 
    try:
        repo_m.insert(conn, ticket_id, "in", sent.message_id, getattr(message, "caption", None), message.content_type)
    finally:
        conn.close()
