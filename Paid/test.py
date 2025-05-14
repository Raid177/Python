import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

# ==== Змінні ====
BOT_TOKEN = "7129977699:AAFBM0oV8H3pYhj7T9uhbzI5d3dLPr3ICsE"

# ==== Логування ====
logging.basicConfig(
    format='[%(asctime)s] %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ==== Обробник ====
async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    log.info("🛠️ handle_file запущено")

    if not msg:
        log.info("❌ msg = None")
        return

    log.info(f"📩 msg.text = {msg.text}")
    log.info(f"📩 msg.caption = {msg.caption}")

    is_pay_command = False
    file_msg = None

    caption = msg.caption.lower() if msg.caption else ""
    if "/оплата" in caption or "/pay" in caption:
        is_pay_command = True
        file_msg = msg
        log.info("✅ Команду визначено через caption")

    # Перевірка reply
    if not is_pay_command and msg.reply_to_message:
        replied = msg.reply_to_message
        log.info(f"📎 reply_to_message.caption = {replied.caption}")
        log.info(f"📎 reply_to_message.document = {replied.document is not None}")
        rep_caption = replied.caption.lower() if replied.caption else ""

        if "/оплата" in rep_caption or "/pay" in rep_caption:
            is_pay_command = True
            file_msg = replied
            log.info("✅ Команду визначено через caption у reply")
        elif replied.document:
            is_pay_command = True
            file_msg = replied
            log.info("✅ Команду визначено через документ у reply")

    # Якщо не знайдено команду — вийти
    if not is_pay_command:
        log.info("⚠️ Команда не визначена. Пропускаємо.")
        return

    if file_msg and file_msg.document:
        log.info(f"📥 Прийнято документ: {file_msg.document.file_name}")
        await file_msg.reply_text("✅ Прийнято документ з командою /оплата")
    else:
        await msg.reply_text("⚠️ Очікується документ як файл")

# ==== Запуск ====
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.ALL, handle_file))
    log.info("🤖 Тестовий бот запущено")
    app.run_polling()
