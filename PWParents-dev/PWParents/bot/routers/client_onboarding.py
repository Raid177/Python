# bot/routers/client_onboarding.py
from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import (
    Message, ChatMemberUpdated, ReplyKeyboardMarkup, KeyboardButton
)

from core.config import settings
from core.db import get_conn
from core.repositories import clients as repo_c
from core.repositories import tickets as repo_t
from bot.keyboards.common import ticket_actions_kb

router = Router(name="client_onboarding")

# ---------- helpers ----------

def _phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поділитись телефоном", request_contact=True)],
            [KeyboardButton(text="➡️ Пропустити")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

async def _ensure_ticket_and_topic(bot: Bot, client_id: int) -> int:
    """
    Гарантує відкритий тікет і тему у саппорт-групі. Повертає message_thread_id.
    """
    conn = get_conn()
    try:
        # 1) шукаємо відкритий тікет
        #   використовуй свою реалізацію repo_t.get_open_by_client(...)
        t = repo_t.get_open_by_client(conn, client_id)
        if t and t.get("thread_id"):
            return int(t["thread_id"])

        # якщо тікета немає — створимо
        if not t:
            ticket_id = repo_t.create(conn, client_user_id=client_id)
        else:
            ticket_id = t["id"]

        # 2) створюємо тему
        try:
            ch = await bot.get_chat(client_id)
            username = f"@{ch.username}" if getattr(ch, "username", None) else str(client_id)
        except Exception:
            username = str(client_id)

        topic = await bot.create_forum_topic(
            chat_id=settings.support_group_id,
            name=f"Новий клієнт {username}",
        )
        thread_id = topic.message_thread_id

        # 3) фіксуємо thread_id у БД
        repo_t.set_thread(conn, ticket_id, thread_id)

        # 4) службова картка у тему
        await bot.send_message(
            chat_id=settings.support_group_id,
            message_thread_id=thread_id,
            text=(
                "🟢 Заявка\n"
                f"Клієнт: <code>{username}</code>\n"
                "Статус: open"
            ),
            reply_markup=ticket_actions_kb(client_id),
        )

        return thread_id
    finally:
        conn.close()

async def _welcome_flow(bot: Bot, user_id: int) -> None:
    """
    Ідемпотентний вітальний флоу:
    - гарантує клієнта у БД
    - гарантує тікет/тему (без шуму)
    - якщо телефону ще не було і не просили — просить поділитись
    """
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, user_id)
        client = repo_c.get_client(conn, user_id)
        conn.commit()
    finally:
        conn.close()

    # тема/тікет
    await _ensure_ticket_and_topic(bot, user_id)

    # якщо вже є телефон — нічого не просимо
    if client and client.get("phone"):
        return

    # якщо ще не просили номер — попросимо і запишемо штамп
    if not client or not client.get("last_phone_prompt_at"):
        await bot.send_message(
            chat_id=user_id,
            text=(
                "Вітаємо у клініці PetWealth! 💚\n"
                "Напишіть зручний день/час, ім’я пацієнта та причину звернення.\n\n"
                "Щоб ми могли ідентифікувати вас і надсилати нагадування — поділіться номером телефону."
            ),
            reply_markup=_phone_kb(),
        )
        conn = get_conn()
        try:
            # використовуй свій хелпер; якщо його немає — зроби UPDATE поля last_phone_prompt_at=NOW()
            repo_c.touch_last_phone_prompt(conn, user_id)
            conn.commit()
        finally:
            conn.close()

# ---------- handlers ----------

# 1) Автоонбординг при натисканні Start у боті (my_chat_member)
@router.my_chat_member(F.chat.type == "private")
async def on_my_chat_member(event: ChatMemberUpdated, bot: Bot):
    old = event.old_chat_member.status
    new = event.new_chat_member.status
    if old in ("left", "kicked") and new == "member" and event.from_user:
        await _welcome_flow(bot, event.from_user.id)

# 2) Підстраховка — звичайний /start (на випадок, якщо my_chat_member не прилетів)
@router.message(F.chat.type == "private", Command("start"))
async def start_cmd(message: Message, bot: Bot):
    await _welcome_flow(bot, message.from_user.id)
