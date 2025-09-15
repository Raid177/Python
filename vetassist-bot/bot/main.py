# main.py
# Завдання: запуск Telegram-бота. Підключаємо команди /start, /help, /kontrol

import asyncio
from telegram.ext import Application, CommandHandler
from modules.config import settings
from .handlers import help_cmd, kontrol_cmd, pick_cmd  # додали pick_cmd
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from .handlers import help_cmd, kontrol_cmd, pick_cmd, number_pick_fallback
import logging





async def start_cmd(update, context):
    await update.message.reply_text("Вітаю! Бот VetAssist працює. Команд поки що мало.")

async def main():
    logging.basicConfig(level=logging.INFO)

    if not settings.tg_bot_token:
        raise RuntimeError("BOT_TOKEN_VetAssist не знайдено у .env.prod")

    app = Application.builder().token(settings.tg_bot_token).build()

    # Команди
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("kontrol", kontrol_cmd))
    app.add_handler(CommandHandler("pick", pick_cmd))  # запасний варіант вибору
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, number_pick_fallback))

    # гарантуємо що вебхук відключено
    await app.bot.delete_webhook(drop_pending_updates=True)

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    print("✅ VetAssist бот запущено. Очікую команди /start, /help, /kontrol.")

    try:
        await asyncio.Event().wait()
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        
if __name__ == "__main__":
    asyncio.run(main())
    
