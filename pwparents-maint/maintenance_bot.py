import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import os

# === КОНФІГ ===
BOT_TOKEN = os.getenv("BOT_TOKEN", "ТОКЕН_ТУТ")  # можна підставити вручну або експортувати
MESSAGE = (
    "⚙️ Проводяться технічні роботи.\n"
    "Будь ласка, зачекайте кілька хвилин.\n\n"
    "Якщо питання термінове — телефонуйте 📞 (044) 33 44 55 1"
)

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    # Команди типу /start
    @dp.message(Command(commands=["start", "help"]))
    async def start(msg: types.Message):
        await msg.answer(MESSAGE)

    # Будь-яке інше повідомлення
    @dp.message()
    async def echo(msg: types.Message):
        await msg.answer(MESSAGE)

    print("�� Maintenance bot запущено. Натисни Ctrl+C щоб зупинити.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
