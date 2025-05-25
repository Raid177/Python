<<<<<<< HEAD
# === telegram_bot_payments.py ===
# Включає: антидубль, обробку файлів з /оплата, БД, кнопки, баланс, моніторинг статусу, видалення

import os
import sys
import time
import tempfile
import atexit
import msvcrt
import pymysql
import psutil
import requests
import shutil
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CallbackQueryHandler,
    CommandHandler, ContextTypes, filters, DeletedMessageHandler
)

# === 🔒 Вбивання дублюючих процесів ===
def kill_duplicates():
    time.sleep(2)
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

# === 🌱 Змінні середовища ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
BOT_TOKEN = os.getenv("BOT_TOKEN")
FALLBACK_CHAT_ID = int(os.getenv("FALLBACK_CHAT_ID"))
ADMIN_USER = int(os.getenv("ADMIN_USER", FALLBACK_CHAT_ID))
ALLOWED_USERS = os.getenv("ALLOWED_USERS", str(FALLBACK_CHAT_ID))
if ALLOWED_USERS == "*":
    ALLOWED_USERS_SET = "*"
else:
    ALLOWED_USERS_SET = set(map(int, ALLOWED_USERS.split(",")))

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")
SAVE_DIR = "C:/Users/la/OneDrive/Рабочий стол/На оплату!"
DELETED_DIR = os.path.join(SAVE_DIR, "Deleted")
LOG_FILE = "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Paid/from_telegram_log.txt"

# === 💾 БД ===
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
payment_notified = set()

# === 📋 Баланси ===
API_URL = "https://acp.privatbank.ua/api/statements/balance"
accounts = {}
for var in os.environ:
    if var.startswith("API_TOKEN_"):
        fop = var.replace("API_TOKEN_", "")
        token = os.getenv(var)
        acc_list = os.getenv(f"API_АСС_{fop}", "").split(",")
        acc_list = [acc.strip() for acc in acc_list if acc.strip()]
        accounts[fop] = [{"acc": acc, "token": token} for acc in acc_list]

def get_today_balance(account, token):
    today = datetime.now().date()
    headers = {"User-Agent": "PythonClient", "token": token, "Content-Type": "application/json;charset=cp1251"}
    params = {"acc": account, "startDate": today.strftime("%d-%m-%Y"), "endDate": today.strftime("%d-%m-%Y")}
    try:
        response = requests.get(API_URL, headers=headers, params=params)
        if response.status_code != 200:
            return None, f"❌ Помилка {response.status_code}: {response.text}"
        data = response.json()
        if data["status"] != "SUCCESS" or "balances" not in data:
            return None, "⚠️ Дані відсутні або статус не SUCCESS."
        return data["balances"][0], None
    except Exception as e:
        return None, f"❗ Виняток: {str(e)}"

def format_amount(value):
    try:
        parts = f"{value:,.2f}".split(".")
        int_part = parts[0].replace(",", "\u00a0")
        return f"{int_part},{parts[1]}"
    except:
        return str(value)

def build_balance_report():
    total_uah = 0
    report_lines = []
    for fop, acc_list in accounts.items():
        for acc_info in acc_list:
            bal, err = get_today_balance(acc_info["acc"], acc_info["token"])
            if err:
                report_lines.append(f"*{fop}*: {err}")
            elif bal:
                try:
                    value = float(bal["balanceOut"])
                    if value == 0:
                        continue
                    formatted = format_amount(value)
                    line = f"*{fop}*\n💳 `{bal['acc']}`\n💰 *{formatted} {bal['currency']}*"
                    report_lines.append(line)
                    if bal["currency"] == "UAH":
                        total_uah += value
                except Exception as e:
                    report_lines.append(f"*{fop}*: ⚠️ Помилка при обробці: {e}")
    if total_uah:
        report_lines.append(f"\n📊 *Загальна сума (UAH)*: *{format_amount(total_uah)} грн*")
    return "\n\n".join(report_lines)

# === 🔐 Перевірка доступу ===
def is_allowed(uid):
    return ALLOWED_USERS_SET == "*" or uid in ALLOWED_USERS_SET

# === 🪵 Логи ===
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# === 🧠 Моніторинг оплати ===
async def check_paid_loop(app):
    while True:
        cursor.execute("SELECT id, file_name, chat_id, message_id FROM telegram_files WHERE status = 'paid' ORDER BY updated_at DESC LIMIT 20")
        for row in cursor.fetchall():
            fid, fname, chat_id, msg_id = row
            if fid not in payment_notified:
                try:
                    await app.bot.send_message(chat_id=chat_id, reply_to_message_id=msg_id, text=f"✅ Файл *{fname}* оплачено.", parse_mode="Markdown")
                    payment_notified.add(fid)
                    log(f"💸 Повідомлено про оплату: {fname}")
                except Exception as e:
                    log(f"⚠️ Не вдалося надіслати повідомлення про оплату {fname}: {e}")
        await asyncio.sleep(10)

# === 🗑️ Обробка видалення повідомлення ===
async def handle_deleted(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg_id = update.message_id
    chat_id = update.effective_chat.id
    cursor.execute("SELECT id, file_name, file_path, status FROM telegram_files WHERE chat_id=%s AND message_id=%s", (chat_id, msg_id))
    row = cursor.fetchone()
    if row:
        fid, fname, fpath, status = row
        if status == 'paid':
            await context.bot.send_message(chat_id=chat_id, text=f"🔒 Файл *{fname}* вже оплачено. Видалення заборонено.", parse_mode="Markdown")
            return
        cursor.execute("UPDATE telegram_files SET status='deleted', deleted_by='user', deleted_at=NOW() WHERE id=%s", (fid,))
        os.makedirs(DELETED_DIR, exist_ok=True)
        try:
            shutil.move(fpath, os.path.join(DELETED_DIR, os.path.basename(fpath)))
            log(f"🗑️ Файл {fname} переміщено у Deleted")
        except Exception as e:
            log(f"⚠️ Не вдалося перемістити файл {fname}: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🗑️ Файл *{fname}* видалено та переміщено у архів.", parse_mode="Markdown")

# === 📥 Прийом документів, збереження, дублікати ===
async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    caption = msg.caption.lower() if msg.caption else ""
    is_pay_command = "/оплата" in caption or "/pay" in caption
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
        cursor.execute("SELECT timestamp, status FROM telegram_files WHERE file_name=%s AND username=%s ORDER BY id DESC LIMIT 1", (orig_name, user))
        prev = cursor.fetchone()
        if prev:
            prev_time, prev_status = prev
            if prev_status == "paid":
                await msg.reply_text("🔒 Цей файл вже оплачено. Повторне надсилання заборонено.")
                return
            sessions[user] = (file, msg)
            keyboard = [[InlineKeyboardButton("✅ Зберегти ще раз", callback_data="save_again"), InlineKeyboardButton("❌ Скасувати", callback_data="cancel")]]
            await msg.reply_text(f"⚠️ Файл «{orig_name}» вже надсилався {prev_time.strftime('%Y-%m-%d %H:%M:%S')}. Повторити збереження?", reply_markup=InlineKeyboardMarkup(keyboard))
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
    """, (base_name, file_path, msg.chat.id, msg.message_id, user, now))

    note = " (дубль)" if is_duplicate else ""
    await msg.reply_text(f"📥 Файл «{base_name}»{note} передано на оплату")
    log(f"✔️ {base_name} збережено {note}")

# === 🔘 Кнопки дубля ===
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
        log(f"🚫 Користувач {user} скасував дубль")] ...

# === 📊 Команда /balance ===
async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != ADMIN_USER:
        return await update.message.reply_text("⛔ Доступ лише для адміністратора.")
    msg = build_balance_report()
    await update.message.reply_text(msg, parse_mode="Markdown")

# === 🚀 Старт ===
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(DeletedMessageHandler(handle_deleted))
    asyncio.create_task(check_paid_loop(app))
    log("🤖 Бот запущено...")
    app.run_polling()
=======
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
>>>>>>> 5b4823abe3a1ac38974ee560e33c2c669b16e8dd
