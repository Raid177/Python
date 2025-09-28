from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from core.db import get_conn
from core.repositories import messages as repo_m
from core.repositories import tickets as repo_t
from bot.keyboards.common import ticket_actions_kb

async def _log_in(conn, ticket_id:int, tg_msg_id:int, text:str|None, media_type:str):
    repo_m.insert(conn, ticket_id, "in", tg_msg_id, text, media_type)

async def _post_restored_notice_and_card(bot: Bot, support_group_id:int, thread_id:int, ticket_id:int):
    conn = get_conn()
    try:
        t = repo_t.get_by_id(conn, ticket_id)
    finally:
        conn.close()
    if not t:
        return
    # службове повідомлення для саппорту
    await bot.send_message(
        chat_id=support_group_id,
        message_thread_id=thread_id,
        text=f"♻️ Тему відновлено для клієнта <code>{t['client_user_id']}</code> (ticket #{ticket_id}).",
    )
    # картка з кнопками
    await bot.send_message(
        chat_id=support_group_id,
        message_thread_id=thread_id,
        text=(f"🟢 Заявка\nКлієнт: <code>{t['client_user_id']}</code>\n"
              f"Статус: {t['status']}"),
        reply_markup=ticket_actions_kb(t["client_user_id"]),
    )

async def log_and_send_text_to_topic(bot: Bot, support_group_id:int, thread_id:int, ticket_id:int, text:str, head:str):
    try:
        sent = await bot.send_message(chat_id=support_group_id, message_thread_id=thread_id, text=f"{head}\n\n{text}")
    except TelegramBadRequest as e:
        if "message thread not found" in str(e).lower():
            # дістати client_id щоб назвати тему ID<client_id>
            conn = get_conn()
            try:
                t = repo_t.get_by_id(conn, ticket_id)
                client_id = t["client_user_id"] if t else None
            finally:
                conn.close()
            topic = await bot.create_forum_topic(
                chat_id=support_group_id,
                name=f"ID{client_id or ticket_id}"
            )
            # оновити thread_id
            conn = get_conn()
            try:
                repo_t.update_thread(conn, ticket_id, topic.message_thread_id)
            finally:
                conn.close()
            # службове повідомлення + картка
            await _post_restored_notice_and_card(bot, support_group_id, topic.message_thread_id, ticket_id)
            # відправити текст
            sent = await bot.send_message(chat_id=support_group_id, message_thread_id=topic.message_thread_id, text=f"{head}\n\n{text}")
        else:
            raise
    conn = get_conn()
    try:
        await _log_in(conn, ticket_id, sent.message_id, text, "text")
    finally:
        conn.close()

async def log_inbound_media_copy(message, support_group_id:int, thread_id:int, ticket_id:int, head:str, bot: Bot):
    try:
        sent = await message.copy_to(chat_id=support_group_id, message_thread_id=thread_id, caption=head)
    except TelegramBadRequest as e:
        if "message thread not found" in str(e).lower():
            conn = get_conn()
            try:
                t = repo_t.get_by_id(conn, ticket_id)
                client_id = t["client_user_id"] if t else None
            finally:
                conn.close()
            topic = await bot.create_forum_topic(
                chat_id=support_group_id,
                name=f"ID{client_id or ticket_id}"
            )
            conn = get_conn()
            try:
                repo_t.update_thread(conn, ticket_id, topic.message_thread_id)
            finally:
                conn.close()
            await _post_restored_notice_and_card(bot, support_group_id, topic.message_thread_id, ticket_id)
            sent = await message.copy_to(chat_id=support_group_id, message_thread_id=topic.message_thread_id, caption=head)
        else:
            raise
    conn = get_conn()
    try:
        repo_m.insert(conn, ticket_id, "in", sent.message_id, getattr(message, "caption", None), message.content_type)
    finally:
        conn.close()
