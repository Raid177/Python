from aiogram import Router, Bot, F
from aiogram.types import Message
from aiogram.filters import Command, CommandObject
from core.config import settings
from core.db import get_conn
from core.repositories.agents import upsert_agent, set_display_name, get_display_name

router = Router()

async def _is_member(bot: Bot, user_id:int) -> bool:
    try:
        cm = await bot.get_chat_member(settings.support_group_id, user_id)
        return cm.status in ("member","administrator","creator")
    except Exception:
        return False

@router.message(Command("who"))
async def who(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@router.message(Command("start"))
async def start_agent(message: Message, bot: Bot):
    if not await _is_member(bot, message.from_user.id):
        await message.answer("Привіт! Щоб працювати як співробітник, спершу додайся до службової групи з темами.")
        return
    conn = get_conn()
    try:
        current = get_display_name(conn, message.from_user.id)
    finally:
        conn.close()
    if current:
        await message.answer(f"Вітаю! Твоє відображуване ім'я: <b>{current}</b>.\nМожеш змінити: /setname Імʼя Прізвище")
    else:
        await message.answer("Вітаю! Ти в групі. Обери імʼя, яке бачитимуть клієнти:\n/setname Імʼя Прізвище")

@router.message(Command("setname"))
async def setname(message: Message, command: CommandObject, bot: Bot):
    if not await _is_member(bot, message.from_user.id):
        await message.answer("Ця команда доступна лише співробітникам групи."); return
    name = (command.args or "").strip()
    if not name:
        await message.answer("Використання: /setname Імʼя Прізвище"); return
    conn = get_conn()
    try:
        upsert_agent(conn, message.from_user.id, name)
    finally:
        conn.close()
    await message.answer(f"✅ Імʼя збережено: <b>{name}</b>")
