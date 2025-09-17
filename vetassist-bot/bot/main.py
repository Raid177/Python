# bot/main.py
import asyncio
import logging
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from modules.config import settings
from .handlers import help_cmd, kontrol_cmd, pick_cmd, number_pick_fallback  # start_cmd оголошуємо тут

async def start_cmd(update, context):
    await update.message.reply_text("Вітаю! Бот VetAssist працює. Команд поки що мало.")

async def main():
    # Логування: приглушуємо зайве
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

    if not settings.tg_bot_token:
        raise RuntimeError("BOT_TOKEN_VetAssist не знайдено у .env.prod")

    app = Application.builder().token(settings.tg_bot_token).build()

    # --- Команди ---
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("kontrol", kontrol_cmd))
    app.add_handler(CommandHandler("pick", pick_cmd))

    # --- Текстові хендлери ---
    # Спершу ловимо повідомлення, де /kontrol стоїть у будь-якому місці (початок/кінець/посередині)
    app.add_handler(
    MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Regex(r"(?i)(?:^|[\s,.;])\/kontrol(?:$|[\s,.;])"),
        kontrol_cmd
    )
)

    # Потім — простий числовий вибір (щоб не перехоплював /kontrol-повідомлення)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, number_pick_fallback))

    # На всяк випадок відключимо вебхук перед poll’інгом
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
