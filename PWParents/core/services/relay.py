# core/services/relay.py
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from core.db import get_conn
from core.repositories import tickets as repo_t
from bot.keyboards.common import ticket_actions_kb
from bot.service.msglog import log_and_touch


async def _post_restored_notice_and_card(bot: Bot, support_group_id: int, thread_id: int, ticket_id: int):
    conn = get_conn()
    try:
        t = repo_t.get_by_id(conn, ticket_id)
    finally:
        conn.close()
    if not t:
        return

    await bot.send_message(
        chat_id=support_group_id,
        message_thread_id=thread_id,
        text=f"♻️ Тему відновлено для клієнта <code>{t['client_user_id']}</code> (ticket #{ticket_id}).",
    )
    await bot.send_message(
        chat_id=support_group_id,
        message_thread_id=thread_id,
        text=(f"🟢 Заявка\nКлієнт: <code>{t['label'] or t['client_user_id']}</code>\n"
              f"Статус: {t['status']}"),
        reply_markup=ticket_actions_kb(t["client_user_id"]),
    )


async def log_and_send_text_to_topic(bot: Bot, support_group_id: int, thread_id: int, ticket_id: int, text: str, head: str):
    try:
        sent = await bot.send_message(chat_id=support_group_id, message_thread_id=thread_id, text=f"{head}\n\n{text}")
    except TelegramBadRequest as e:
        if "message thread not found" in str(e).lower():
            conn = get_conn()
            try:
                t = repo_t.get_by_id(conn, ticket_id)
                client_id = t["client_user_id"] if t else None
            finally:
                conn.close()
            topic = await bot.create_forum_topic(chat_id=support_group_id, name=f"ID{client_id or ticket_id}")
            conn = get_conn()
            try:
                repo_t.update_thread(conn, ticket_id, topic.message_thread_id)
            finally:
                conn.close()
            await _post_restored_notice_and_card(bot, support_group_id, topic.message_thread_id, ticket_id)
            sent = await bot.send_message(chat_id=support_group_id, message_thread_id=topic.message_thread_id, text=f"{head}\n\n{text}")
        else:
            raise

    # лог + touch_client
    log_and_touch(ticket_id, "in", sent.message_id, text, "text")


async def log_inbound_media_copy(message, support_group_id: int, thread_id: int, ticket_id: int, head: str, bot: Bot):
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
            topic = await bot.create_forum_topic(chat_id=support_group_id, name=f"ID{client_id or ticket_id}")
            conn = get_conn()
            try:
                repo_t.update_thread(conn, ticket_id, topic.message_thread_id)
            finally:
                conn.close()
            await _post_restored_notice_and_card(bot, support_group_id, topic.message_thread_id, ticket_id)
            sent = await message.copy_to(chat_id=support_group_id, message_thread_id=topic.message_thread_id, caption=head)
        else:
            raise

    # лог + touch_client (caption може бути None — це ок)
    log_and_touch(ticket_id, "in", sent.message_id, getattr(message, "caption", None), message.content_type)
