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
logger = logging.getLogger(__name__)

router = Router()  # ← СТАВИМО ПЕРЕД ДЕКОРАТОРАМИ

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
        # НЕ відповідаємо, і головне — НЕ блокуємо (завдяки flags={"block": False})
        return

       # гарантуємо запис у БД
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

@router.message(Command("setname"), F.chat.type == "private")
async def setname_private(message: Message, command: CommandObject):
    logger.info("SETNAME hit: uid=%s text=%r", message.from_user.id, message.text)
    name = (command.args or "").strip()
    if not name:
        await message.answer("Використання: <code>/setname Імʼя Прізвище</code>")
        return

    uid = message.from_user.id
    conn = get_conn()
    try:
        repo_a.upsert_agent(conn, telegram_id=uid, display_name=name, role="doctor", active=1)
        conn.commit()
    finally:
        conn.close()

    await message.answer(f"Готово ✅ Ваше ім’я для клієнтів: <b>{name}</b>")

@router.message(Command("who"), F.chat.type == "private")
async def who_private(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@router.message(Command("name"), F.chat.type == "private")
async def name_private(message: Message):
    display = get_display_name(get_conn(), message.from_user.id)
    await message.answer(f"Поточне ім’я для клієнтів: <b>{display or '— не задано —'}</b>")
