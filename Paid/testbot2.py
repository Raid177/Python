# === testbot.py ===

# 📦 Імпорти
import os
import logging
from datetime import datetime
from dotenv import dotenv_values
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# 🕒 Час запуску — для розрахунку аптайму
start_time = datetime.now()

# 🎛️ Головне меню (reply клавіатура)
main_keyboard = ReplyKeyboardMarkup(
    [["/start", "/checkbot"], ["/help", "/balance"]],
    resize_keyboard=True
)

# === 🔐 Конфігурація з .env ===
env = dotenv_values("/root/Python/.env")
BOT_TOKEN = env["TESTBOT_TOKEN"]
FALLBACK_CHAT_ID = int(env["FALLBACK_TESTCHAT_ID"])
LOG_FILE_Test = env.get("LOG_FILE_Test", "/root/Python/Paid/test_log.py")

# === 👥 Ролі користувачів ===
ROLE_ADMIN = [161197876]
ROLE_MANAGER = []

# === 📜 Логування
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE_Test, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# === Обробка помилок глобально ===
async def error_handler(update, context):
    logger.error(f"❌ Виникла помилка: {context.error}")
    if update:
        try:
            await update.message.reply_text("⚠️ Виникла внутрішня помилка. Адміністратор вже знає.")
        except Exception:
            pass  # якщо навіть повідомлення не можна надіслати

# === 🔍 Визначення ролі користувача
def get_user_role(user_id: int) -> str:
    if user_id in ROLE_ADMIN:
        return "admin"
    elif user_id in ROLE_MANAGER:
        return "manager"
    else:
        return "employee"

# === 🟢 /start — вивід ролі користувача
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    role = get_user_role(user.id)
    message = f"👋 Привіт, {user.first_name}! Ваша роль: {role}"
    logger.info(f"/start від {user.id} ({user.username}) — роль: {role}")
    await update.message.reply_text(message, reply_markup=main_keyboard)

# === ✅ /checkbot — одразу відповідає статусом і аптаймом
async def checkbot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = datetime.now() - start_time
    seconds = int(uptime.total_seconds())
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = f"{hours} год {minutes} хв {seconds} с"

    text = f"✅ Бот онлайн\n⏱ Аптайм: {uptime_str}"
    logger.info(f"/checkbot від {update.effective_user.id} ({update.effective_user.username}) — {uptime_str}")
    await update.message.reply_text(text)

# === ⌨️ /checkbutton — надсилає кнопку "перевірити бота"
async def checkbutton_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔍 Перевірити бота", callback_data="check_bot")]]
    )
    await update.message.reply_text(
        "Натисни кнопку нижче для перевірки стану бота:",
        reply_markup=keyboard
    )
    logger.info(f"/checkbutton від {update.effective_user.id} ({update.effective_user.username})")

# === 📩 Обробка натискання на кнопку
async def handle_check_bot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    uptime = datetime.now() - start_time
    seconds = int(uptime.total_seconds())
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = f"{hours} год {minutes} хв {seconds} с"

    text = f"✅ Бот онлайн\n⏱ Аптайм: {uptime_str}"
    logger.info(f"🔘 check_bot натиснуто від {query.from_user.id} ({query.from_user.username}) — {uptime_str}")
    await query.edit_message_text(text=text)

# === 🚀 MAIN — точка входу
def main():
    logger.info("🚀 Запуск TEST бота...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # ✅ Команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("checkbot", checkbot_command))         # одразу статус
    app.add_handler(CommandHandler("checkbutton", checkbutton_command))   # кнопка
    app.add_handler(CallbackQueryHandler(handle_check_bot_callback, pattern="^check_bot$"))

    app.run_polling()

# 🧪 Запуск
def main():
    logger.info("🚀 Запуск TEST бота...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("checkbot", checkbot_command))
    app.add_handler(CommandHandler("checkbutton", checkbutton_command))
    app.add_handler(CallbackQueryHandler(handle_check_bot_callback, pattern="^check_bot$"))

    app.add_error_handler(error_handler)

    try:
        app.run_polling()
    except Exception as e:
        logger.critical(f"🔥 Бот аварійно завершив роботу: {e}")

if __name__ == "__main__":
    main()
