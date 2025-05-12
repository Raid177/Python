import os
import sys
import time
import tempfile
import atexit
import msvcrt
import pymysql
import psutil
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity
from telegram.ext import ApplicationBuilder, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# === 🔒 Блокування другого екземпляра (пізніше замінюється кілером) ===
def kill_duplicates():
    time.sleep(2)  # Пауза після запуску, щоб дати другому процесу з'явитись
    current_pid = os.getpid()
    this_name = os.path.basename(sys.argv[0])
    for p in psutil.process_iter(['pid', 'name', 'exe', 'cmdline']):
        try:
            cmdline = p.info.get('cmdline') or []
            if isinstance(cmdline, list) and this_name in ' '.join(cmdline) and p.pid != current_pid:
                p.kill()
                print(f"🛑 Вбито дубльований процес {this_name} (PID {p.pid})")
        except Exception as e:
            print(f"⚠️ Не вдалося завершити PID {p.pid}: {e}")

kill_duplicates()

# === 🧪 .env змінні ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")
SAVE_DIR = "C:/Users/la/OneDrive/Рабочий стол/На оплату!"
LOG_FILE = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Paid/from_telegram_log.txt"

# === 🔌 Підключення до БД ===
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

# === 🪵 Логи ===
def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# === 📥 Обробка документів ===
async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    caption = msg.caption.lower() if msg.caption else ""
    is_pay_command = "/оплата" in caption or "/pay" in caption

    # Якщо reply на /оплата
    if not is_pay_command and msg.reply_to_message:
        for ent in msg.reply_to_message.entities or []:
            if ent.type == MessageEntity.BOT_COMMAND:
                command = msg.reply_to_message.text[ent.offset: ent.offset + ent.length]
                if command.lower() in ("/оплата", "/pay"):
                    is_pay_command = True
                    break

    if not is_pay_command:
        return

    user = msg.from_user.username or "anon"

    if msg.document:
        file = msg.document
        orig_name = file.file_name

        # Перевірка дубля
        cursor.execute("SELECT timestamp FROM telegram_files WHERE file_name=%s AND username=%s ORDER BY id DESC LIMIT 1", (orig_name, user))
        prev = cursor.fetchone()

        if prev:
            prev_time = prev[0].strftime("%Y-%m-%d %H:%M:%S")
            sessions[user] = (file, msg)
            keyboard = [[
                InlineKeyboardButton("✅ Зберегти ще раз", callback_data="save_again"),
                InlineKeyboardButton("❌ Скасувати", callback_data="cancel")
            ]]
            await msg.reply_text(f"⚠️ Файл «{orig_name}» вже надсилався {prev_time}. Повторити збереження?", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await save_and_record(file, msg, context, user, is_duplicate=False)

    else:
        await msg.reply_text("🚫 Тип файлу не підтримується. Надсилайте лише як документ (PDF, Excel, JPG, TXT тощо).")

# === 💾 Збереження файлу ===
async def save_and_record(file, msg, context, user, is_duplicate):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_name = file.file_name
    if is_duplicate:
        safe_name = f"{os.path.splitext(base_name)[0]}__DUPLICATE_{ts}{os.path.splitext(base_name)[1]}"
    else:
        safe_name = base_name

    file_path = os.path.join(SAVE_DIR, safe_name)
    os.makedirs(SAVE_DIR, exist_ok=True)

    telegram_file = await context.bot.get_file(file.file_id)
    await telegram_file.download_to_drive(file_path)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""
        INSERT INTO telegram_files (file_name, file_path, chat_id, message_id, username, timestamp, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'pending')
    """, (
        base_name, file_path, msg.chat.id, msg.message_id, user, now
    ))

    note = " (дубль)" if is_duplicate else ""
    await msg.reply_text(f"📥 Файл «{base_name}»{note} передано на оплату")
    log(f"✔️ {base_name} збережено {note}")

# === 🧷 Обробка кнопок "Зберегти повторно" ===
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user.username or "anon"

    if user not in sessions:
        await query.edit_message_text("⚠️ Сесію не знайдено")
        return

    file, msg = sessions.pop(user)
    if query.data == "save_again":
        await save_and_record(file, msg, context, user, is_duplicate=True)
        await query.edit_message_text("✅ Повторне збереження виконано")
    else:
        await query.edit_message_text("❌ Скасовано")
        log(f"🚫 Користувач {user} скасував дубль")

# === 🚀 Старт бота ===
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.Document.ALL | filters.ALL, handle_file))
    app.add_handler(CallbackQueryHandler(handle_callback))
    log("🤖 Бот запущено...")
    app.run_polling()
