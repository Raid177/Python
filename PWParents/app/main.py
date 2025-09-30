# app/main.py
import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from core.config import settings
from infra.logging import setup_logging

from bot.routers import root
from bot.routers import health  # ок залишити, якщо health НЕ всередині root

from bot.service.reminder import start_idle_reminder
from bot.auth import acl_refresher_task

async def main():
    setup_logging(settings.log_level)
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)

    bot = Bot(
        settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()

    # лічильник апдейтів
    dp.message.middleware(health.count_updates_middleware)

    # маршрути — БЕЗ дублювань
    dp.include_router(health.router)   # ← і health (якщо не в root)
    dp.include_router(root.router)     # ← тільки root
    

    await bot.delete_webhook(drop_pending_updates=True)

    if settings.REMINDER_ENABLED:
        asyncio.create_task(start_idle_reminder(bot))

    asyncio.create_task(acl_refresher_task(bot))

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
