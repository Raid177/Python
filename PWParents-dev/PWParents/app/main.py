# app/main.py
import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand
from aiogram.types import (
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
    BotCommandScopeChat,
)

from core.config import settings
from core.db import get_conn
from infra.logging import setup_logging

from bot.routers import root
from bot.routers import health  # лічильник апдейтів-мідлвар і /health роутер
from bot.service.reminder import start_idle_reminder
from bot.auth import acl_refresher_task
from bot.service.phone_reminder import start_phone_reminders

async def setup_staff_private_commands(bot: Bot) -> None:
    """
    Ставимо персональну підказку /setname у приваті ТІЛЬКИ співробітникам.
    Беремо список активних агентів з pp_agents.active=1 і задаємо BotCommandScopeChat(chat_id=<agent_id>).
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT telegram_id FROM pp_agents WHERE active=1")
            ids = [row[0] for row in cur.fetchall() if row and row[0]]
    finally:
        conn.close()

    if not ids:
        return

    staff_private_cmds = [
        BotCommand(command="setname", description="Змінити ваше ім’я для клієнтів"),
    ]

    for uid in ids:
        try:
            await bot.set_my_commands(
                commands=staff_private_cmds,
                scope=BotCommandScopeChat(chat_id=uid),
            )
        except Exception:
            logging.getLogger(__name__).warning(
                "Cannot set private commands for staff user_id=%s (maybe blocked bot)", uid
            )


async def setup_bot_commands(bot: Bot) -> None:
    # Приватні (клієнти)
    await bot.set_my_commands(
        commands=[
            BotCommand(command="start", description="Почати / головне меню"),
            BotCommand(command="menu", description="Показати меню"),
            BotCommand(command="help", description="Як користуватися ботом"),
        ],
        scope=BotCommandScopeAllPrivateChats(),
    )

    # Саппорт-група — основні
    await bot.set_my_commands(
        commands=[
            BotCommand(command="assign", description="Призначити виконавця"),
            BotCommand(command="close", description="Закрити звернення"),
            BotCommand(command="close_silent", description="Тихо закрити (без клієнта)"),
            BotCommand(command="card", description="Картка звернення в тему"),
            BotCommand(command="client", description="Дані клієнта"),
            BotCommand(command="phone", description="Телефон(и) клієнта"),
            BotCommand(command="threadinfo", description="IDs поточної теми"),
            BotCommand(command="label", description="Задати мітку теми"),
            BotCommand(command="reopen", description="Перевідкрити звернення"),
            BotCommand(command="snooze", description="Відкласти алерти: /snooze 30"),
            BotCommand(command="enote_link", description="Прив’язати клієнта до власника (Єнот)"),
            BotCommand(command="patient", description="Пацієнти власника"),
            BotCommand(command="auto_label", description="Автоперейменувати тему"),
            
            # ── службові/технічні в самому кінці ──
            BotCommand(command="status", description="Статус бота"),
            BotCommand(command="version", description="Версія релізу"),
            BotCommand(command="ping", description="Пінг"),
            BotCommand(command="whoami", description="Хто я"),
            BotCommand(command="acl_reload", description="Перечитати ACL"),
            BotCommand(command="test", description="Тест"),
            BotCommand(command="wipe_client_dry", description="(DEV) Показати, що буде видалено"),
            BotCommand(command="wipe_client",     description="(DEV) Видалити клієнта + тему (НЕЗВОРОТНО)"),


        ],
        scope=BotCommandScopeChat(chat_id=settings.support_group_id),
    )

    # За замовчуванням — для всіх груп (мінімум)
    await bot.set_my_commands(
        commands=[BotCommand(command="threadinfo", description="IDs поточної теми")],
        scope=BotCommandScopeAllGroupChats(),
    )
    # Персональні приватні команди для співробітників (лише /setname)
    await setup_staff_private_commands(bot)


async def main():
    # Логування
    setup_logging(settings.log_level)
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)

    # Бот і диспетчер
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    # Мідлвар лічильника апдейтів
    dp.message.middleware(health.count_updates_middleware)

    # Маршрути — без дублювання
    dp.include_router(health.router)
    dp.include_router(root.router)

    # Чистимо вебхук і ставимо команди
    await bot.delete_webhook(drop_pending_updates=True)
    await setup_bot_commands(bot)

    # Фонові задачі
    if settings.REMINDER_ENABLED:
        asyncio.create_task(start_idle_reminder(bot))
        logging.getLogger("bot.service.reminder").info(
            "reminder: starting (idle=%sm, ping=%sm, escalate=%s)",
            settings.REMINDER_IDLE_MINUTES,
            settings.REMINDER_PING_EVERY_MIN,
            settings.ESCALATE_UNASSIGNED,
        )
    # ⬇️ НОВЕ: нагадування про телефон
    if settings.PHONE_REMINDER_ENABLED:
        asyncio.create_task(start_phone_reminders(bot))

    # Старт полінгу
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
