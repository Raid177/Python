# bot/routers/client_onboarding.py

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, ChatMemberUpdated, ReplyKeyboardMarkup, KeyboardButton

from core.config import settings
from core.db import get_conn
from core.repositories import clients as repo_c

# ⬇️ додай цей імпорт — беремо єдину імплементацію створення теми/тікета
from bot.routers.client import _ensure_ticket_for_client

router = Router(name="client_onboarding")

def _phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поділитись телефоном", request_contact=True)],
            [KeyboardButton(text="➡️ Пропустити")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

async def _welcome_flow(bot: Bot, user_id: int) -> None:
    # 1) гарантуємо запис клієнта
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, user_id)
        conn.commit()
    finally:
        conn.close()

    # 2) гарантуємо тікет+тему (єдина реалізація)
    await _ensure_ticket_for_client(bot, user_id, silent=True)

    # 3) привітання + прохання телефону
    await bot.send_message(
        chat_id=user_id,
        text=(
            "Вітаємо у клініці PetWealth! 💚\n"
            "Напишіть зручний день/час, ім’я пацієнта та причину звернення.\n\n"
            "Щоб ми ідентифікували вас і надсилали нагадування — поділіться номером телефону."
        ),
        reply_markup=_phone_kb(),
    )

@router.my_chat_member(F.chat.type == "private")
async def on_my_chat_member(event: ChatMemberUpdated, bot: Bot):
    old = event.old_chat_member.status
    new = event.new_chat_member.status
    if old in ("left", "kicked") and new == "member" and event.from_user:
        await _welcome_flow(bot, event.from_user.id)

@router.message(F.chat.type == "private", Command("start"))
async def start_cmd(message: Message, bot: Bot):
    await _welcome_flow(bot, message.from_user.id)
