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
import mimetypes
import pymysql

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

# === Підключення до БД ===
def get_db_connection():
    return pymysql.connect(
        host=env["DB_HOST"],
        user=env["DB_USER"],
        password=env["DB_PASSWORD"],
        database=env["DB_DATABASE"],
        cursorclass=pymysql.cursors.DictCursor
    )

# === Дозволені типи файлів ===
ALLOWED_EXTENSIONS = {'.pdf', '.xls', '.xlsx', '.txt', '.jpeg', '.jpg', '.png'}

# === SAVE_DIR ===
SAVE_DIR = env.get("SAVE_DIR_test", "/root/Automation/Paid/test")

# === Обробка /pay через файл (caption або reply) ===
async def handle_payment_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    message = update.effective_message
    username = user.username or "unknown"

    # 🔐 Перевірка ролі
    role = get_user_role(user.id)
    if role not in {"admin", "manager"}:
        logger.warning(f"⛔ Недостатньо прав: {user.id} ({username})")
        return

    # 📌 Перевірка чи є команда /pay або /оплата
    is_triggered = False
    if message.caption:
        is_triggered = any(x in message.caption.lower() for x in ["/pay", "/оплата"])
    if message.reply_to_message and message.text and any(x in message.text.lower() for x in ["/pay", "/оплата"]):
        is_triggered = True
        message = message.reply_to_message  # беремо файл з reply

    if not is_triggered or not message.document:
        logger.info(f"ℹ️ Пропуск: {user.id} ({username}) — без тригеру або без файлу")
        return

    file = message.document
    original_filename = file.file_name
    ext = os.path.splitext(original_filename)[1].lower()

    # 📎 Перевірка дозволеного формату
    if ext not in ALLOWED_EXTENSIONS:
        reply = "⚠️ Для оплати передайте файл у форматі: PDF, Excel, TXT, PNG, JPEG"
        await update.message.reply_text(reply)
        logger.warning(f"⚠️ Непідтримуваний формат: {original_filename}")
        return

    # 🧾 Перевірка в БД
    conn = get_db_connection()
    with conn.cursor() as cursor:
        sql = "SELECT * FROM telegram_files WHERE file_name = %s ORDER BY created_at DESC LIMIT 1"
        cursor.execute(sql, (original_filename,))
        existing = cursor.fetchone()

    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    save_name = original_filename
    if existing:
        base, ext = os.path.splitext(original_filename)
        save_name = f"{base}_copy_{now_str}{ext}"

    os.makedirs(SAVE_DIR, exist_ok=True)
    file_path = os.path.join(SAVE_DIR, save_name)

    # 💾 Збереження файлу
    tg_file = await context.bot.get_file(file.file_id)
    await tg_file.download_to_drive(file_path)
    logger.info(f"📥 Збережено файл: {file_path}")

    # 🧮 Вставка в БД
    with conn.cursor() as cursor:
        sql = """
        INSERT INTO telegram_files (file_name, file_path, chat_id, message_id, username, timestamp, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), 'pending', NOW(), NOW())
        """
        cursor.execute(sql, (
            original_filename,
            file_path,
            chat.id,
            message.message_id,
            username
        ))
    conn.commit()
    conn.close()

    # 📩 Відповідь у Telegram
    if existing:
        sent_at = existing['created_at'].strftime("%Y-%m-%d %H:%M")
        reply = (
            f"⚠️ Файл з такою назвою вже надсилався {sent_at} "
            f"користувачем @{existing['username']}.\n"
            f"✅ Відправлено повторно з новою назвою: {save_name}"
        )
    else:
        reply = "✅ Прийнято до сплати. Очікуйте повідомлення про оплату."

    await update.message.reply_text(reply)
    logger.info(f"✅ /pay — {user.id} ({username}) — файл: {original_filename}")

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

    app.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.GROUPS | filters.ChatType.PRIVATE, handle_payment_file))


    try:
        app.run_polling()
    except Exception as e:
        logger.critical(f"🔥 Бот аварiйно зупинився: {e}")

if __name__ == "__main__":
    main()