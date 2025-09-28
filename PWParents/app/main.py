import asyncio
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from core.config import settings
from infra.logging import setup_logging
from bot.routers import client, staff, callbacks
from bot.routers import client, staff, callbacks, agents

async def main():
    setup_logging(settings.log_level)

    bot = Bot(
        settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()
    dp.include_router(client.router)
    dp.include_router(staff.router)
    dp.include_router(callbacks.router)
    dp.include_router(client.router)
    dp.include_router(staff.router)
    dp.include_router(callbacks.router)
    dp.include_router(agents.router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
