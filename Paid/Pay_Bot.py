import os
import tempfile
import msvcrt
import sys
import atexit
from datetime import datetime
import pymysql
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# === Блокування запуску другого екземпляра ===
script_name = os.path.basename(sys.argv[0])
lockfile_name = f"{os.path.splitext(script_name)[0]}.lock"
lockfile_path = os.path.join(tempfile.gettempdir(), lockfile_name)
try:
    lock_file = open(lockfile_path, 'w')
    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
except OSError:
    print("⚠️ Бот уже запущений. Вихід.")
    sys.exit()

def cleanup():
    try:
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        lock_file.close()
        os.remove(lockfile_path)
    except Exception:
        pass
atexit.register(cleanup)

# === Конфігурація ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")
SAVE_DIR = "C:/Users/la/OneDrive/Рабочий стол/На оплату!"
LOG_FILE = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Paid/from_telegram_log.txt"
SUPPORTED_EXT = [".pdf", ".xls", ".xlsx", ".txt", ".jpg", ".jpeg", ".png", ".bmp", ".tiff"]

# === Підключення до БД ===
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE,
    charset='utf8mb4',
    autocommit=True
)
cursor = conn.cursor()
sessions = {}

def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

async def save_and_record(file, msg, context, user, is_duplicate):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_name = file.file_name
    if is_duplicate:
        name_part, ext = os.path.splitext(base_name)
        safe_name = f"{name_part}__DUPLICATE_{ts}{ext}"
    else:
        safe_name = base_name

    file_path = os.path.join(SAVE_DIR, safe_name)
    os.makedirs(SAVE_DIR, exist_ok=True)

    telegram_file = await context.bot.get_file(file.file_id)
    await telegram_file.download_to_drive(file_path)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""
        INSERT INTO telegram_files (file_name, file_path, chat_id, message_id, username, timestamp, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'pending')
    """, (
        base_name, file_path,
        msg.chat.id, msg.message_id,
        user, timestamp
    ))

    note = " (дубль)" if is_duplicate else ""
    await msg.reply_text(f"📥 Файл {base_name}{note} передано на оплату")
    log(f"✔️ {base_name} збережено {'(дубль)' if is_duplicate else ''}")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user.username or "anon"
    if user not in sessions:
        await query.edit_message_text("⚠️ Сесію не знайдено або вона завершилась")
        return

    file, msg = sessions.pop(user)
    if query.data == "save_again":
        await save_and_record(file, msg, context, user, is_duplicate=True)
        await query.edit_message_text("✅ Повторне збереження виконано")
    else:
        await query.edit_message_text("❌ Повторне збереження скасовано")
        log(f"🚫 Користувач {user} скасував дубль")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    user = msg.from_user.username or "anon"
    caption = msg.caption or ""
    text = msg.text or ""
    has_pay = "/оплата" in caption.lower() or "/pay" in caption.lower() or "/оплата" in text.lower()

    # 1️⃣ /оплата з файлом
    if has_pay and msg.document:
        await process_document(msg.document, msg, context, user)
        return

    # 2️⃣ /оплата у reply на файл
    if has_pay and msg.reply_to_message and msg.reply_to_message.document:
        reply_doc = msg.reply_to_message.document
        await process_document(reply_doc, msg.reply_to_message, context, user)
        return

    # 3️⃣ /оплата з фото, відео, іншим — попередження
    if has_pay:
        await msg.reply_text("⚠️ Цей тип повідомлення не підтримується. Надсилайте файл як документ (PDF, Excel, TXT або зображення).")

async def process_document(file, msg, context, user):
    ext = os.path.splitext(file.file_name)[1].lower()
    if ext not in SUPPORTED_EXT:
        await msg.reply_text("⚠️ Цей тип файлу не підтримується. Надсилайте PDF, Excel, TXT або зображення як файл.")
        return

    cursor.execute("""
        SELECT timestamp FROM telegram_files
        WHERE file_name = %s AND username = %s
        ORDER BY id DESC LIMIT 1
    """, (file.file_name, user))
    duplicate = cursor.fetchone()

    if duplicate:
        prev_time = duplicate[0].strftime("%Y-%m-%d %H:%M:%S")
        sessions[user] = (file, msg)
        keyboard = [[
            InlineKeyboardButton("✅ Так, зберегти повторно", callback_data="save_again"),
            InlineKeyboardButton("❌ Ні, скасувати", callback_data="cancel")
        ]]
        await msg.reply_text(
            f"⚠️ Файл '{file.file_name}' вже надсилався {prev_time}. Зберегти повторно?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    await save_and_record(file, msg, context, user, is_duplicate=False)

if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.ALL, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    log("🤖 Бот запущено...")
    app.run_polling()
