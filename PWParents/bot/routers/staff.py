# bot/routers/staff.py
import logging
from datetime import datetime, timedelta

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import html

from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command, CommandObject
from aiogram.exceptions import TelegramBadRequest

from core.config import settings
from core.db import get_conn
from core.repositories import (
    messages as repo_m,
    tickets as repo_t,
    agents as repo_a,
    clients as repo_c,
)
from core.repositories.agents import get_display_name

from bot.keyboards.common import (
    prefix_for_staff,
    ticket_actions_kb,
    assign_agents_kb,
)
from bot.routers._media import relay_media
from bot.utils.staff_guard import IsSupportMember

router = Router()
logger = logging.getLogger(__name__)

# =========================
# СЛУЖБОВІ КОМАНДИ В ТЕМІ
# =========================

@router.message(Command("label"), F.chat.id == settings.support_group_id, F.is_topic_message == True)
async def set_label_cmd(message: Message, command: CommandObject, bot: Bot):
    new_label = (command.args or "").strip()
    if not new_label:
        await message.answer("Використання: /label Ім'я_клієнта_або_тварини")
        return
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t:
            return
        repo_t.set_label(conn, t["id"], new_label)
    finally:
        conn.close()

    try:
        await bot.edit_forum_topic(
            chat_id=settings.support_group_id,
            message_thread_id=message.message_thread_id,
            name=new_label[:128],
        )
    except Exception:
        pass

    await message.answer(f"✅ Мітку теми оновлено на: <b>{new_label}</b>")

@router.message(
    Command("assign"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
@router.message(
    Command("assign"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def assign_cmd(message: Message, command: CommandObject, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
    finally:
        conn.close()
    if not t:
        return

    label = t.get("label") or t["client_user_id"]
    args = (command.args or "").strip()

    # 1) без аргументів — показати список співробітників
    if not args:
        conn = get_conn()
        try:
            agents = repo_a.list_active(conn)
        finally:
            conn.close()

        if not agents:
            await message.answer(
                "Список співробітників порожній. Додайте їх у pp_agents або нехай виконають /setname у приваті."
            )
            return

        kb = assign_agents_kb(agents, client_id=t["client_user_id"], exclude_id=None)
        await message.answer(f"Кому передати клієнта <b>{html.escape(str(label))}</b>?", reply_markup=kb)
        return

    # 2) пряме призначення за Telegram ID
    if args.isdigit():
        tg_id = int(args)
        conn = get_conn()
        try:
            repo_t.assign_to(conn, t["id"], tg_id)
            repo_t.set_status(conn, t["id"], "in_progress")
        finally:
            conn.close()

        who = get_display_name(get_conn(), tg_id) or tg_id
        safe_label = html.escape(str(label))

        # повідомлення в тему (як було)
        await message.answer(f"🟡 Призначено виконавця: <b>{who}</b> для клієнта <b>{safe_label}</b>")

        # приватне повідомлення виконавцю з клікабельною кнопкою "Відкрити тему"
        try:
            topic_url = await build_topic_url(bot, settings.support_group_id, t["thread_id"])
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➡️ Відкрити тему", url=topic_url)]
            ])
            await bot.send_message(
                chat_id=tg_id,
                text=(
                    f"🔔 Вам призначено звернення клієнта <b>{safe_label}</b>.\n"
                    f"Натисніть кнопку, щоб перейти у тему."
                ),
                reply_markup=kb,
                disable_web_page_preview=True
            )
        except Exception:
            await message.answer("ℹ️ Не вдалося надіслати приватне повідомлення (співробітник не стартував бота).")
        return

    # 3) help
    await message.answer(
        "Використання:\n"
        "• /assign 123456789 — одразу призначити за Telegram ID\n"
        "• /assign — показати список співробітників і обрати з кнопок"
    )

@router.message(
    Command("close"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def staff_close(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t: return
        repo_t.close_ticket(conn, t["id"])
    finally:
        conn.close()

    await message.answer("🔴 Звернення закрито.")
    await bot.send_message(
        chat_id=t["client_user_id"],
        text="✅ Щиро дякуємо за довіру. Якщо знадобиться допомога — пишіть.",
    )

@router.message(
    Command("reopen"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
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

@router.message(
    Command("close_silent"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def close_silent_cmd(message: Message, bot: Bot):
    try:
        conn = get_conn()
        try:
            t = repo_t.find_by_thread(conn, message.message_thread_id)
            if not t:
                await message.answer("ℹ️ Для цієї теми немає активного звернення.")
                return

            repo_t.close_ticket(conn, t["id"])
            repo_t.clear_snooze(conn, t["id"])
            conn.commit()
        finally:
            conn.close()

        await message.answer("🔴 Закрито тихо (без повідомлення клієнту).")

    except Exception as e:
        logger.exception("close_silent failed: %s", e)
        await message.answer("⚠️ Не вдалось закрити тихо (див. логи).")

@router.message(
    Command("card"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def post_card(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
    finally:
        conn.close()
    if not t: return
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

@router.message(
    Command("client", "phone"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def show_client_info(message: Message, bot: Bot):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t:
            await message.answer("ℹ️ Не знайшов тікет, прив’язаний до цієї теми.")
            return
        c = repo_c.get_client(conn, t["client_user_id"])
    finally:
        conn.close()

    # --- отримуємо username з Telegram ---
    try:
        ch = await bot.get_chat(t["client_user_id"])
        if getattr(ch, "username", None):
            # клікабельний лінк на профіль
            tg_username = f"<a href='https://t.me/{ch.username}'>@{ch.username}</a>"
        else:
            tg_username = "— відсутній —"
    except Exception:
        tg_username = "— недоступний —"

    # --- інша інформація ---
    label = (c and c.get("label")) or t["client_user_id"]
    phone = (c and c.get("phone")) or "— не вказано —"
    confirmed = None
    if c is not None:
        pc = c.get("phone_confirmed")
        if pc is None:
            confirmed = " "
        else:
            confirmed = "підтверджено ✅" if int(pc) == 1 else "не підтверджено ❌"
    total_closed = (c and c.get("total_closed")) or 0
    tg_link = f"tg://user?id={t['client_user_id']}"

    # --- формуємо текст відповіді ---
    text = (
        "<b>Картка клієнта</b>\n"
        f"• Клієнт: <code>{label}</code>\n"
        f"• Telegram ID: <a href='{tg_link}'>{t['client_user_id']}</a>\n"
        f"• Нік: {tg_username}\n"
        f"• Телефон: <code>{phone}</code>"
    )
    if confirmed is not None:
        text += f" ({confirmed})"
    text += f"\n• Закритих звернень: <b>{total_closed}</b>"

    await message.answer(text, disable_web_page_preview=True)

# =============== ПРОКСІ ІЗ ТЕМИ → КЛІЄНТУ ===============
@router.message(
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    (F.text & ~F.text.startswith("/")) | ~F.text,
    IsSupportMember(),
    flags={"block": False},
)
async def outbound_to_client(message: Message, bot: Bot):
    if message.from_user.is_bot:
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

    if not t.get("assigned_to"):
        conn = get_conn()
        try:
            repo_t.assign_to(conn, t["id"], message.from_user.id)
            repo_t.set_status(conn, t["id"], "in_progress")
        finally:
            conn.close()
        who = get_display_name(get_conn(), message.from_user.id) or message.from_user.id
        try:
            await bot.send_message(
                chat_id=message.chat.id,
                message_thread_id=message.message_thread_id,
                text=f"🟡 Автопризначення: <b>{who}</b>",
            )
        except Exception:
            pass

    fallback = message.from_user.full_name or (message.from_user.username and f"@{message.from_user.username}") or None
    prefix = prefix_for_staff(message.from_user.id, fallback=fallback)

    from bot.service.msglog import log_and_touch

    if message.content_type == "text":
        out = await bot.send_message(chat_id=t["client_user_id"], text=f"{prefix}\n\n{message.text}")
        log_and_touch(t["id"], "out", out.message_id, message.text, "text")
    else:
        out = await relay_media(bot, message, t["client_user_id"], prefix=prefix)
        log_and_touch(t["id"], "out", out.message_id, getattr(message, "caption", None), message.content_type)

@router.message(Command("threadinfo"), F.chat.type == "supergroup")
async def thread_info(message: Message):
    tid = message.message_thread_id
    if not tid:
        await message.answer("ℹ️ Це не гілка (або команда поза темою).")
        return
    await message.answer(
        f"<b>Thread info</b>\n"
        f"Chat ID: <code>{message.chat.id}</code>\n"
        f"Thread ID: <code>{tid}</code>\n"
        f"(Назву теми Telegram API не віддає)",
        disable_web_page_preview=True,
    )

@router.message(
    Command("snooze"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def snooze_cmd(message: Message, command: CommandObject):
    args = (command.args or "").strip()
    if not args or not args.isdigit():
        await message.answer("Використання: <code>/snooze 30</code> — відкласти алерти на 30 хв.")
        return

    mins = int(args)
    if mins < 1 or mins > 1440:
        await message.answer("Значення має бути від 1 до 1440 хв.")
        return

    until_dt = datetime.utcnow() + timedelta(minutes=mins)

    conn = get_conn()
    try:
        t = repo_t.get_by_thread(conn, message.message_thread_id)
        if not t:
            return
        repo_t.set_snooze_until(conn, t["id"], until_dt)
        conn.commit()
    finally:
        conn.close()

    await message.answer(f"⏸ Алерти вимкнено до <b>{until_dt:%Y-%m-%d %H:%M UTC}</b>.")

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import html

# утиліта для правильного лінку на тему
def _abs_chat_id_str(chat_id: int) -> str:
    s = str(chat_id)
    if s.startswith("-100"):
        return s[4:]
    if s.startswith("-"):
        return s[1:]
    return s

async def build_topic_url(bot: Bot, group_id: int, thread_id: int) -> str:
    """Повертає пряме посилання на тему (forum topic) у службовій групі."""
    ch = await bot.get_chat(group_id)
    if getattr(ch, "username", None):  # публічна група
        return f"https://t.me/{ch.username}/{thread_id}"
    return f"https://t.me/c/{_abs_chat_id_str(ch.id)}/{thread_id}"
