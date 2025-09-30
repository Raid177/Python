# bot/routers/agents.py
from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command, CommandObject
from aiogram.exceptions import TelegramBadRequest

from core.config import settings
from core.db import get_conn
from core.repositories import agents as repo_a
from core.repositories.agents import get_display_name

import logging
import re  # ← потрібен для _clean_name
logger = logging.getLogger(__name__)

router = Router()

def _is_staff_member_status(status: str) -> bool:
    return status in ("creator", "administrator", "member")

@router.message(Command("start"), F.chat.type == "private", flags={"block": False})
async def start_private(message: Message, bot: Bot):
    u = message.from_user
    # перевірка членства у support-групі
    is_staff = False
    try:
        cm = await bot.get_chat_member(settings.support_group_id, u.id)
        is_staff = cm.status in ("creator", "administrator", "member")
    except TelegramBadRequest:
        pass

    if not is_staff:
        # НЕ відповідаємо і НЕ блокуємо
        return

    # гарантуємо запис у БД (порожнє ім'я ок на старті)
    conn = get_conn()
    try:
        repo_a.upsert_agent(conn, telegram_id=u.id, display_name="", role="doctor", active=1)
        conn.commit()
    finally:
        conn.close()

    display = get_display_name(get_conn(), u.id)
    if display:
        await message.answer(
            f"Вітаю! Ти в команді PetWealth 🐾\n"
            f"• Твоє ім’я для клієнтів: <b>{display}</b>\n"
            f"• Твій Telegram ID: <a href='tg://user?id={u.id}'>{u.id}</a>\n"
            f"• У темі групи можна використовувати /assign, /label, /close тощо."
        )
    else:
        await message.answer(
            "Вітаю! Ти в команді PetWealth 🐾\n"
            "• Задай ім’я, яке бачитимуть клієнти: /setname Імʼя Прізвище\n"
            f"• Твій Telegram ID: <a href='tg://user?id={u.id}'>{u.id}</a>\n"
            "• У темі групи можна використовувати /assign, /label, /close тощо."
        )

def _clean_name(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _is_active_agent(conn, telegram_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pp_agents WHERE telegram_id=%s AND active=1 LIMIT 1", (telegram_id,))
        return cur.fetchone() is not None

@router.message(Command("setname"), F.chat.type == "private")
async def setname_private(message: Message, command: CommandObject, bot: Bot):
    uid = message.from_user.id

    # допускаємо, якщо юзер у support-групі АБО вже активний у БД
    allowed = False
    try:
        cm = await bot.get_chat_member(settings.support_group_id, uid)
        allowed = cm.status in ("creator", "administrator", "member")
    except TelegramBadRequest as e:
        logger.warning("get_chat_member failed: %s", e)

    if not allowed:
        conn = get_conn()
        try:
            allowed = _is_active_agent(conn, uid)
        finally:
            conn.close()

    if not allowed:
        return  # тихо ігноруємо для сторонніх

    # парсимо ім'я
    name = _clean_name(command.args or "")
    if not name:
        await message.answer("Використання: <code>/setname Імʼя Прізвище</code>")
        return
    if len(name) > 50:
        await message.answer("❌ Надто довге ім’я (макс 50 символів).")
        return

    # зберігаємо display_name
    conn = get_conn()
    try:
        # Вибери ОДИН з варіантів виклику згідно з репозиторієм:

        # ВАРІАНТ А: якщо ти реалізуєш апсерт-версію з activate=
        repo_a.set_display_name(conn, telegram_id=uid, display_name=name, activate=True)

        # ВАРІАНТ B (закоментуй А): якщо залишаєш стару функцію без activate
        # спершу гарантуємо запис, потім оновлюємо поле
        # repo_a.upsert_agent(conn, telegram_id=uid, display_name="", role="doctor", active=1)
        # repo_a.set_display_name(conn, telegram_id=uid, display_name=name)

        conn.commit()
    except Exception:
        logger.exception("setname failed")
        await message.answer("⚠️ Не вдалось оновити імʼя (БД). Спробуйте пізніше.")
        return
    finally:
        conn.close()

    await message.answer(f"Готово ✅ Ваше ім’я для клієнтів: <b>{name}</b>")
