from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command, CommandObject
from aiogram.exceptions import TelegramBadRequest

from core.config import settings
from core.db import get_conn
from core.repositories import messages as repo_m
from core.repositories import tickets as repo_t
from core.repositories.agents import get_display_name
from bot.keyboards.common import prefix_for_staff, ticket_actions_kb
from bot.routers._media import relay_media

router = Router()

# Відповіді з теми → клієнту (ігноруємо команди/службові)
@router.message(F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def outbound_to_client(message: Message, bot: Bot):
    if message.from_user.is_bot:
        return
    if message.text and message.text.startswith("/"):
        return
    if message.new_chat_members or message.left_chat_member or message.pinned_message:
        return

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

# /label — мітка для заголовків (Від клієнта …)
@router.message(Command("label"), F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def set_label_cmd(message: Message, command: CommandObject, bot: Bot):
    new_label = (command.args or "").strip()
    if not new_label:
        await message.answer("Використання: /label Ім'я_клієнта_або_тварини"); return
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t: return
        repo_t.set_label(conn, t["id"], new_label)
    finally:
        conn.close()
    await message.answer(f"✅ Мітку теми оновлено на: <b>{new_label}</b>")

# /assign <telegram_id> — призначити відповідального + DM
@router.message(Command("assign"), F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def assign_cmd(message: Message, command: CommandObject, bot: Bot):
    args = (command.args or "").strip()
    if not args or not args.isdigit():
        await message.answer("Використання: /assign <telegram_id>\n(P.S. співробітник може дізнатись свій ID командою /who у приваті)"); return
    tg_id = int(args)

    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t: return
        repo_t.assign_to(conn, t["id"], tg_id)
        repo_t.set_status(conn, t["id"], "in_progress")
        label = t.get("label") or t["client_user_id"]
    finally:
        conn.close()

    who = get_display_name(get_conn(), tg_id) or tg_id
    await message.answer(f"🟡 Призначено виконавця: <b>{who}</b> для клієнта <b>{label}</b>")

    try:
        await bot.send_message(
            chat_id=tg_id,
            text=(f"🔔 Вам призначено звернення клієнта <b>{label}</b>.\n"
                  f"Зайдіть у тему в службовій групі й відповідайте від свого імені.")
        )
    except Exception:
        await message.answer("ℹ️ Не вдалося надіслати приватне повідомлення (співробітник не стартував бота).")

# /close — закрити звернення
@router.message(Command("close"), F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def staff_close(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t: return
        repo_t.close_ticket(conn, t["id"])
    finally:
        conn.close()
    await message.answer("🔴 Звернення закрито.")
    await bot.send_message(chat_id=t["client_user_id"], text="✅ Звернення закрито. Напишіть будь-коли — продовжимо в цій же темі.")

# /reopen — перевідкрити вручну
@router.message(Command("reopen"), F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def staff_reopen(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t: return
        repo_t.reopen(conn, t["id"])
    finally:
        conn.close()
    await message.answer("🟢 Перевідкрито.")
    await bot.send_message(
        chat_id=message.chat.id,
        message_thread_id=message.message_thread_id,
        text=f"🟢 Перевідкрито | Клієнт: <code>{t['label'] or t['client_user_id']}</code>",
        reply_markup=ticket_actions_kb(t["client_user_id"]),
    )

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
            text=(f"🟢 Заявка\nКлієнт: <code>{t['label'] or t['client_user_id']}</code>\n"
                  f"Статус: {t['status']}"),
            reply_markup=ticket_actions_kb(t["client_user_id"]),
        )
    except TelegramBadRequest:
        pass
