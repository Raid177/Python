import os
import tempfile
import msvcrt
import sys
import atexit
from datetime import datetime
import pymysql
from dotenv import load_dotenv
from telegram import Update, MessageEntity
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes

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

# === Розблокування при виході ===
def cleanup():
    try:
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        lock_file.close()
        os.remove(lockfile_path)
    except Exception:
        pass
atexit.register(cleanup)

# === Завантаження конфігурації ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")
SAVE_DIR = "C:/Users/la/OneDrive/Рабочий стол/На оплату!"
LOG_FILE = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Paid/from_telegram_log.txt"

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

# === Логування ===
def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# === Обробка вхідних файлів ===
async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    caption = msg.caption.lower() if msg.caption else ""

    if "/оплата" not in caption and "/pay" not in caption:
        return

    if not msg.document:
        await msg.reply_text("⚠️ Будь ласка, надішліть файл як документ із командою /оплата")
        log("Відхилено: повідомлення без документа")
        return

    file = msg.document
    file_name = file.file_name
    file_path = os.path.join(SAVE_DIR, file_name)

    os.makedirs(SAVE_DIR, exist_ok=True)
    telegram_file = await context.bot.get_file(file.file_id)
    await telegram_file.download_to_drive(file_path)
    log(f"📥 Отримано файл: {file_name}, збережено в: {file_path}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""
        INSERT INTO telegram_files (file_name, file_path, chat_id, message_id, username, timestamp, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'pending')
    """, (
        file_name, file_path,
        msg.chat.id, msg.message_id,
        msg.from_user.username or "", timestamp
    ))
    log(f"🗂️ Запис додано в БД для {file_name} (chat_id={msg.chat.id}, message_id={msg.message_id})")

    await msg.reply_text(f"📥 Файл {file_name} передано на оплату")

# === Запуск бота ===
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    log("🤖 Бот запущено...")
    app.run_polling()
