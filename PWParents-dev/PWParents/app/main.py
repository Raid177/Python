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
from infra.logging import setup_logging

from bot.routers import root
from bot.routers import health  # лічильник апдейтів-мідлвар і /health роутер

from bot.service.reminder import start_idle_reminder
from bot.auth import acl_refresher_task

async def setup_bot_commands(bot: Bot) -> None:
    """Реєструємо підказки команд (автокомпліт /...) під різні скопи."""

    # Приватні чати (клієнти)
    await bot.set_my_commands(
        commands=[
            BotCommand(command="start", description="Почати / головне меню"),
            BotCommand(command="menu", description="Показати меню"),
            BotCommand(command="help", description="Як користуватися ботом"),
        ],
        scope=BotCommandScopeAllPrivateChats(),
    )

    # Службова група (ваш саппорт-чат)
    await bot.set_my_commands(
        commands=[
            BotCommand(command="assign", description="Призначити виконавця"),
            BotCommand(command="close", description="Закрити звернення"),
            BotCommand(command="close_silent", description="Тихо закрити (без клієнта)"),
            BotCommand(command="card", description="Картка звернення в тему"),
            BotCommand(command="client", description="Дані клієнта в темі"),
            BotCommand(command="phone", description="Телефон клієнта"),
            BotCommand(command="threadinfo", description="IDs поточної теми"),
            BotCommand(command="label", description="Задати мітку теми"),
            BotCommand(command="reopen", description="Перевідкрити звернення"),
            BotCommand(command="snooze", description="Відкласти алерти: /snooze 30"),
            BotCommand(command="enote_link", description="Прив’язати клієнта до власника (Єнот)"),
        ],
        scope=BotCommandScopeChat(chat_id=settings.support_group_id),
    )

    # (опційно) дефолт для всіх груп — якщо бот колись з’явиться ще десь
    await bot.set_my_commands(
        commands=[
            BotCommand(command="threadinfo", description="IDs поточної теми"),
        ],
        scope=BotCommandScopeAllGroupChats(),
    )


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
    asyncio.create_task(acl_refresher_task(bot))

    # Старт полінгу
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
