# bot/routers/staff.py
from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command, CommandObject
from aiogram.exceptions import TelegramBadRequest

from core.config import settings
from core.db import get_conn
from core.repositories import messages as repo_m
from core.repositories import tickets as repo_t
from core.repositories.agents import get_display_name
from core.repositories import agents as repo_a

from bot.keyboards.common import (
    prefix_for_staff,
    ticket_actions_kb,
    assign_agents_kb,
)
from bot.routers._media import relay_media
from bot.utils.staff_guard import IsSupportMember

from core.repositories import clients as repo_c


router = Router()

# =========================
# СЛУЖБОВІ КОМАНДИ В ТЕМІ
# =========================

# /label — мітка для заголовків (Від клієнта …)
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

    # 🔹 спроба перейменувати тему під мітку
    try:
        # Telegram дозволяє 1–128 символів у назві
        await bot.edit_forum_topic(
            chat_id=settings.support_group_id,
            message_thread_id=message.message_thread_id,
            name=new_label[:128]
        )
    except Exception:
        # тихо ігноруємо — тема може бути видалена/недоступна
        pass

    await message.answer(f"✅ Мітку теми оновлено на: <b>{new_label}</b>")

# /assign — без аргументів показує список з БД; з числовим ID — одразу призначає
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

    # 1) Немає аргументів → список активних співробітників
    if not args:
        conn = get_conn()
        try:
            agents = repo_a.list_active(conn)
        finally:
            conn.close()

        if not agents:
            await message.answer(
                "Список співробітників порожній. Додайте їх у pp_agents або нехай вони "
                "виконають /setname у приваті з ботом."
            )
            return

        kb = assign_agents_kb(agents, client_id=t["client_user_id"], exclude_id=None)
        await message.answer(f"Кому передати клієнта <b>{label}</b>?", reply_markup=kb)
        return

    # 2) Якщо аргумент число → одразу призначити за tg_id
    if args.isdigit():
        tg_id = int(args)

        conn = get_conn()
        try:
            repo_t.assign_to(conn, t["id"], tg_id)
            repo_t.set_status(conn, t["id"], "in_progress")
        finally:
            conn.close()

        who = get_display_name(get_conn(), tg_id) or tg_id
        await message.answer(f"🟡 Призначено виконавця: <b>{who}</b> для клієнта <b>{label}</b>")

        # спроба надіслати DM виконавцю (може не дійти, якщо не натискав /start)
        try:
            await bot.send_message(
                chat_id=tg_id,
                text=(
                    f"🔔 Вам призначено звернення клієнта <b>{label}</b>.\n"
                    f"Зайдіть у тему в службовій групі й відповідайте від свого імені."
                ),
            )
        except Exception:
            await message.answer("ℹ️ Не вдалося надіслати приватне повідомлення (співробітник не стартував бота).")
        return

    # 3) Підказка
    await message.answer(
        "Використання:\n"
        "• /assign 123456789  — одразу призначити за Telegram ID\n"
        "• /assign            — показати список співробітників і обрати з кнопок\n\n"
        "Порада: співробітник може дізнатись свій ID командою /who у приваті з ботом."
    )


# /close — закрити звернення
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
        if not t:
            return
        repo_t.close_ticket(conn, t["id"])
    finally:
        conn.close()

    await message.answer("🔴 Звернення закрито.")
    await bot.send_message(
        chat_id=t["client_user_id"],
        text="✅ Щиро дякуємо за довіру. Якщо знадобиться допомога — пишіть, будемо раді відповісти.",
    )


# /reopen — перевідкрити вручну
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
        if not t:
            return
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
    if not t:
        return

    try:
        await bot.send_message(
            chat_id=message.chat.id,
            message_thread_id=message.message_thread_id,
            text=(
                f"🟢 Заявка\n"
                f"Клієнт: <code>{t['label'] or t['client_user_id']}</code>\n"
                f"Статус: {t['status']}"
            ),
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
async def show_client_info(message: Message):
    """
    /client або /phone у темі → показати дані клієнта:
    - Telegram ID
    - телефон (якщо є) + статус підтвердження
    - мітку (label)
    - скільки закритих тікетів
    """
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t:
            await message.answer("ℹ️ Не знайшов тікет, прив’язаний до цієї теми.")
            return

        client_id = t["client_user_id"]
        c = repo_c.get_client(conn, client_id)  # очікуємо поля: phone, phone_confirmed, label, total_closed
    finally:
        conn.close()

    label = (c and c.get("label")) or client_id
    phone = (c and c.get("phone")) or "— не вказано —"
    confirmed = None
    if c is not None:
        # phone_confirmed може бути 0/1 або None
        pc = c.get("phone_confirmed")
        if pc is None:
            confirmed = " "
        else:
            confirmed = "підтверджено ✅" if int(pc) == 1 else "не підтверджено ❌"

    total_closed = (c and c.get("total_closed")) or 0

    # зручні лінки
    tg_link = f"tg://user?id={client_id}"

    text = (
        "<b>Картка клієнта</b>\n"
        f"• Клієнт: <code>{label}</code>\n"
        f"• Telegram ID: <a href='{tg_link}'>{client_id}</a>\n"
        f"• Телефон: <code>{phone}</code>"
    )
    if confirmed is not None:
        text += f" ({confirmed})"
    text += f"\n• Закритих звернень: <b>{total_closed}</b>"

    await message.answer(text, disable_web_page_preview=True)


# =====================================
# ПРОКСІ ВІДПОВІДЕЙ ІЗ ТЕМИ → КЛІЄНТУ
# =====================================
#  • НЕ матчимо команди (~F.text.startswith("/"))
#  • НЕ блокуємо інші хендлери (flags={"block": False})
@router.message(
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    (F.text & ~F.text.startswith("/")),
    IsSupportMember(),
    flags={"block": False},
)
async def outbound_to_client(message: Message, bot: Bot):
    if message.from_user.is_bot:
        return
    if message.new_chat_members or message.left_chat_member or message.pinned_message:
        return

    # знайти тікет за thread_id
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
    finally:
        conn.close()
    if not t:
        return

    # 🔸 автопризначення, якщо ще порожньо
    if not t.get("assigned_to"):
        conn = get_conn()
        try:
            repo_t.assign_to(conn, t["id"], message.from_user.id)
            repo_t.set_status(conn, t["id"], "in_progress")
        finally:
            conn.close()
        # опційне службове повідомлення у тему
        who_conn = get_conn()
        try:
            who = get_display_name(who_conn, message.from_user.id) or message.from_user.id
        finally:
            who_conn.close()
        try:
            await bot.send_message(
                chat_id=message.chat.id,
                message_thread_id=message.message_thread_id,
                text=f"🟡 Автопризначення: <b>{who}</b>"
            )
        except Exception:
            pass

    # Префікс з розумним fallback
    fallback = (
        message.from_user.full_name
        or (message.from_user.username and f"@{message.from_user.username}")
        or None
    )
    prefix = prefix_for_staff(message.from_user.id, fallback=fallback)

    from bot.service.msglog import log_and_touch  # імпорт тут, щоб уникати циклічних імпортів

    if message.content_type == "text":
        out = await bot.send_message(
            chat_id=t["client_user_id"],
            text=f"{prefix}\n\n{message.text}",
        )
        # лог + touch_staff
        log_and_touch(t["id"], "out", out.message_id, message.text, "text")
    else:
        out = await relay_media(
            bot,
            message,
            t["client_user_id"],
            prefix=prefix,   # "👩‍⚕️ Ім'я…:" — як у тебе
        )
        log_and_touch(
            t["id"], "out", out.message_id,
            getattr(message, "caption", None),
            message.content_type
        )

# Показати chat_id і thread_id поточної теми
@router.message(Command("threadinfo"), F.chat.type == "supergroup")
async def thread_info(message: Message):
    tid = message.message_thread_id
    if not tid:
        await message.answer("ℹ️ Це не гілка (або команда надіслана поза темою).")
        return
    await message.answer(
        f"<b>Thread info</b>\n"
        f"Chat ID: <code>{message.chat.id}</code>\n"
        f"Thread ID: <code>{tid}</code>\n"
        f"(Назву теми Telegram API не віддає; її видно у шапці в інтерфейсі)",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )