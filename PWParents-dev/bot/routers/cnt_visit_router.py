# bot/routers/cnt_visit_router.py

from __future__ import annotations
import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from core.config import settings
from core.repositories.cnt_chat_repo import get_ticket_by_thread_id
from bot.service.cnt_visit_service import process_ticket_preview

log = logging.getLogger("cnt_visit_router")

router = Router(name="cnt_visit_router")


@router.message(Command("visit_test"))
async def cmd_visit_test(message: Message) -> None:
    """
    Проста перевірка, що модуль підключений.
    """
    await message.reply("ℹ️ Модуль перенесення переписки в Єнот підключено (TEST).")


@router.message(Command("visit_preview"))
async def cmd_visit_preview(message: Message) -> None:
    """
    GPT-агрегація переписки по тікету.
    НІЧОГО не створює в Єноті — лише показує прев’ю.
    """

    # 0) Фіча може бути вимкнена через ENV
    if not settings.VISIT_FEATURE_ENABLED:
        await message.reply("⚠️ Модуль візиту зараз вимкнено (VISIT_FEATURE_ENABLED=0).")
        return

    # 1) Перевіряємо, що ми в саппорт-групі
    if message.chat.id != settings.support_group_id:
        await message.reply("⚠️ Команда доступна тільки в саппорт-групі.")
        return

    # 2) Маємо бути всередині теми
    thread_id = message.message_thread_id
    if not thread_id:
        await message.reply("⚠️ Команду потрібно викликати всередині теми (thread).")
        return

    # 3) Знаходимо тікет за thread_id
    ticket = get_ticket_by_thread_id(thread_id)
    if not ticket:
        await message.reply("⚠️ Для цієї теми тікет не знайдено.")
        return

    await message.reply(
        f"⏳ Обробляю тікет #{ticket['id']} через GPT, це може зайняти кілька секунд…"
    )

    # 4) Викликаємо сервіс прев'ю
    try:
        result = await process_ticket_preview(
            ticket_id=ticket["id"],
            agent_telegram_id=message.from_user.id,
        )
    except Exception as e:
        log.exception("visit_preview: виняток у process_ticket_preview для ticket_id=%s", ticket["id"])
        await message.reply(f"❌ Внутрішня помилка сервісу: <code>{e}</code>")
        return

    # Якщо сервіс повернув None
    if result is None:
        await message.reply("❌ Внутрішня помилка: сервіс повернув порожній результат.")
        return

    # result очікується у форматі:
    # { "ok": True/False, "error": "...", "blocks": {...} }
    if not result.get("ok"):
        err = result.get("error", "unknown_error")
        await message.reply(f"❌ Помилка під час агрегації: <code>{err}</code>")
        return

    blocks = result.get("blocks") or {}

    owner_block = (blocks.get("owner") or "").strip()
    doctor_block = (blocks.get("doctor") or "").strip()
    changes_block = (blocks.get("changes") or "").strip()
    total_messages = blocks.get("total_messages") or 0

    if not (owner_block or doctor_block or changes_block):
        await message.reply("⚠️ GPT не повернув жодного змістовного блоку агрегації.")
        return

    # 5) Формуємо один красивий звіт
    text_lines = [
        f"🧪 <b>Прев’ю візиту для тікета #{ticket['id']}</b>",
        "",
        f"<b>Кількість текстових повідомлень:</b> {total_messages}",
        "",
        "<b>1. Інформація від власника</b>",
        owner_block or "—",
        "",
        "<b>2. Інформація від лікаря</b>",
        doctor_block or "—",
        "",
        "<b>3. Зміни в лікуванні</b>",
        changes_block or "—",
    ]

    text = "\n".join(text_lines)

    await message.reply(text)
