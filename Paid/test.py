import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Налаштування логування
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Команда /status
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user.first_name
    logger.info(f"Команда /status отримана від {user}")
    await update.message.reply_text(f"🔧 Статус бота: запущено для {user}!")

async def main():
    # Створюємо об'єкт бота
    app = ApplicationBuilder().token('7129977699:AAFBM0oV8H3pYhj7T9uhbzI5d3dLPr3ICsE').build()

    # Додаємо обробник команди /status
    app.add_handler(CommandHandler("status", status))

    # Запускаємо бота
    await app.run_polling()

if __name__ == "__main__":
    # Перевіряємо чи існує поточний цикл подій
    try:
        import asyncio
        asyncio.run(main())  # Використовуємо asyncio для запуску основного коду
    except RuntimeError as e:
        logger.error(f"Помилка запуску бота: {e}")
