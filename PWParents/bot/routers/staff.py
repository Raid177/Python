from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest

from core.config import settings
from core.db import get_conn
from core.repositories import messages as repo_m
from core.repositories import tickets as repo_t
from bot.keyboards.common import prefix_for_staff, ticket_actions_kb
from bot.routers._media import relay_media

router = Router()

# Відповіді персоналу з теми → клієнту
@router.message(F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def outbound_to_client(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
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
        out = await relay_media(bot, message, t["client_user_id"], prefix)
        conn = get_conn()
        try:
            repo_m.insert(conn, t["id"], "out", out.message_id, getattr(message, "caption", None), message.content_type)
        finally:
            conn.close()

# /card — картка з кнопками в поточну тему
@router.message(Command("card"), F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def post_card(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
    finally:
        conn.close()
    if not t:
        return
    try:
        await bot.send_message(
            chat_id=message.chat.id,
            message_thread_id=message.message_thread_id,
            text=(f"🟢 Заявка\nКлієнт: <code>{t['client_user_id']}</code>\n"
                  f"Статус: {t['status']}"),
            reply_markup=ticket_actions_kb(t["client_user_id"]),
        )
    except TelegramBadRequest:
        pass

# /close — закрити звернення
@router.message(Command("close"), F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def staff_close(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t:
            return
        repo_t.close_ticket(conn, t["id"])
    finally:
        conn.close()
    await bot.send_message(chat_id=message.chat.id, message_thread_id=message.message_thread_id, text="🔴 Звернення закрито.")
    await bot.send_message(chat_id=t["client_user_id"], text="✅ Звернення закрито. Напишіть будь-коли — продовжимо в цій же темі.")

# /reopen — перевідкрити вручну
@router.message(Command("reopen"), F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def staff_reopen(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t:
            return
        repo_t.reopen(conn, t["id"])
    finally:
        conn.close()
    await bot.send_message(
        chat_id=message.chat.id,
        message_thread_id=message.message_thread_id,
        text=f"🟢 Перевідкрито | Клієнт: <code>{t['client_user_id']}</code>",
        reply_markup=ticket_actions_kb(t["client_user_id"]),
    )
