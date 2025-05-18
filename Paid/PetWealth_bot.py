# === testbot.py (простий варіант з ReplyKeyboardMarkup) ===

import os
import logging
from datetime import datetime
from dotenv import dotenv_values
from telegram import (
    Update, ReplyKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)

# === 🕒 Час запуску
start_time = datetime.now()

# === 🔐 Конфiгурацiя
env = dotenv_values("/root/Python/.env")
BOT_TOKEN = env["TESTBOT_TOKEN"]
FALLBACK_CHAT_ID = int(env["FALLBACK_TESTCHAT_ID"])
LOG_FILE = env.get("LOG_FILE_Test", "/root/Python/Paid/test_log.py")

# === 📜 Логування
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# === 👥 Ролi
ROLE_ADMIN = [161197876]
ROLE_MANAGER = []

# === ⚙️ Команди доступнi в групах
ALLOWED_GROUP_COMMANDS = ["start", "checkbot", "help"]

# === 📋 Команди за ролями
ROLE_COMMANDS = {
    "admin":    ["start", "checkbot", "help", "balance", "pay", "menu"],
    "manager":  ["start", "checkbot", "help", "balance"],
    "employee": ["start", "checkbot", "help"]
}

# === 🔹 Отримати роль

def get_user_role(user_id: int) -> str:
    if user_id in ROLE_ADMIN:
        return "admin"
    elif user_id in ROLE_MANAGER:
        return "manager"
    else:
        return "employee"

# === ⌚ Доступнi команди

def get_available_commands(user_id: int, chat_type: str) -> list[str]:
    role = get_user_role(user_id)
    all_cmds = ROLE_COMMANDS.get(role, [])
    return [cmd for cmd in all_cmds if chat_type == "private" or cmd in ALLOWED_GROUP_COMMANDS]

# === 📊 Клавiатура

def get_keyboard_for_chat(user_id: int, chat_type: str):
    commands = get_available_commands(user_id, chat_type)
    buttons = [[f"/{cmd}" for cmd in commands[i:i+2]] for i in range(0, len(commands), 2)]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# === 🟢 /start

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    role = get_user_role(user.id)
    cmds = get_available_commands(user.id, chat.type)

    logger.info(f"➡️ /start вiд {user.id} ({user.username}) — роль: {role}, чат: {chat.type}")
    msg = f"👋 Привiт, {user.first_name}!\nВаша роль: *{role}*\n\n📋 Доступнi команди:\n"
    msg += "\n".join(f"/{cmd}" for cmd in cmds)

    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=get_keyboard_for_chat(user.id, chat.type))

# === 🔝 /help

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    role = get_user_role(user.id)
    cmds = get_available_commands(user.id, chat.type)

    command_desc = {
        "start": "👋 Привiтання i роль",
        "checkbot": "🟢 Перевiрка стану бота",
        "help": "📖 Це меню",
        "balance": "💰 Баланс",
        "pay": "📋 Завантажити платiж",
        "menu": "🔹 Меню дiй"
    }

    text = f"📋 Доступнi команди для ролi *{role}*:\n"
    for cmd in cmds:
        text += f"/{cmd} — {command_desc.get(cmd, '⚙️')}\n"

    logger.info(f"📖 /help вiд {user.id} ({user.username}) — роль: {role}, чат: {chat.type}")
    await update.message.reply_text(text, parse_mode="Markdown")

# === ✅ /checkbot

async def checkbot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    uptime = datetime.now() - start_time
    seconds = int(uptime.total_seconds())
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = f"{hours} год {minutes} хв {seconds} с"

    logger.info(f"✅ /checkbot вiд {user.id} ({user.username}) — {uptime_str}, чат: {chat.type}")
    await update.message.reply_text(f"✅ Бот онлайн\n🕒 Аптайм: {uptime_str}")

# === 📟 Лог усiх

async def log_everything(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text if update.message else "N/A"
    logger.info(f"[ALL] {user.id} ({user.username}): {text}")

# === ❌ Помилки

async def error_handler(update, context):
    logger.error(f"❌ ПОМИЛКА: {context.error}")
    if update and update.message:
        try:
            await update.message.reply_text("⚠️ Виникла внутрішня помилка.")
        except Exception:
            pass

# === 🚀 MAIN

def main():
    logger.info("🚀 Запуск бота...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("checkbot", checkbot_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.ALL, log_everything))

    app.add_error_handler(error_handler)

    try:
        app.run_polling()
    except Exception as e:
        logger.critical(f"🔥 Бот аварiйно зупинився: {e}")

if __name__ == "__main__":
    main()