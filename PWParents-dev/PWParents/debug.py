import asyncio
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from core.config import settings
import logging

logging.basicConfig(level=logging.DEBUG)


async def main():
    bot = Bot(
        settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    @dp.message()
    async def show_chat_id(message: Message):
        print(f"📌 chat.id = {message.chat.id}")
        await message.answer(f"chat.id = {message.chat.id}")

    print(
        "✅ Debug bot started. Напиши щось у групі або в приват, щоб побачити chat.id"
    )
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
