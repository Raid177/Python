#app/main.py
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from core.config import settings
from infra.logging import setup_logging
from bot.routers import root  # ← тільки root
from bot.service.reminder import start_idle_reminder

async def main():
    setup_logging(settings.log_level)

    bot = Bot(
        settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()
    dp.include_router(root.router)  # ← лише один include

    await bot.delete_webhook(drop_pending_updates=True)
    # стартуємо фонового планера
    if settings.REMINDER_ENABLED:
        asyncio.create_task(start_idle_reminder(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
