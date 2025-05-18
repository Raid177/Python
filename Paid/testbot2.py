# === testbot.py (оновлено: /balance через ACP API + OData) ===

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
import requests
import mimetypes
import pymysql
# 📦 Імпорти для кнопок підтвердження
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler

# === 🕒 Час запуску
start_time = datetime.now()

# === 🔐 Конфiгурацiя
env = dotenv_values("/root/Python/.env")
BOT_TOKEN = env["TESTBOT_TOKEN"]
FALLBACK_CHAT_ID = int(env["FALLBACK_TESTCHAT_ID"])
LOG_FILE = env.get("LOG_FILE_Test", "/root/Python/Paid/test_log.py")

# ПриватБанк
PB_TOKENS = {
    "LOV": env.get("API_TOKEN_LOV"),
    "ZVO": env.get("API_TOKEN_ZVO")
}
PB_ACCOUNTS = {
    "LOV": [env.get("API_АСС_LOV", "")],
    "ZVO": env.get("API_АСС_ZVO", "").split(",")
}

# OData
ODATA_URL = env.get("ODATA_URL")
ODATA_USER = env.get("ODATA_USER")
ODATA_PASSWORD = env.get("ODATA_PASSWORD")
ODATA_ACCOUNTS = {
    "Інкассація (транзитний)": "7e87f26e-eaad-11ef-9d9b-2ae983d8a0f0",
    "Реєстратура каса": "a7dda748-86d1-11ef-839c-2ae983d8a0f0",
    "Каса Організації": "f179f3be-4e84-11ef-83bb-2ae983d8a0f0"
}

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

# === 🎯 Отримати роль
def get_user_role(user_id: int) -> str:
    if user_id in ROLE_ADMIN:
        return "admin"
    elif user_id in ROLE_MANAGER:
        return "manager"
    else:
        return "employee"

# === ⏱️ Доступнi команди

def get_available_commands(user_id: int, chat_type: str) -> list[str]:
    role = get_user_role(user_id)
    all_cmds = ROLE_COMMANDS.get(role, [])
    return [cmd for cmd in all_cmds if chat_type == "private" or cmd in ALLOWED_GROUP_COMMANDS]

# === 🧮 Клавiатура

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

    logger.info(f"➡️ /start від {user.id} ({user.username}) — роль: {role}, чат: {chat.type}")
    msg = f"👋 Привіт, {user.first_name}!\nВаша роль: *{role}*\n\n📋 Доступні команди:\n"
    msg += "\n".join(f"/{cmd}" for cmd in cmds)

    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=get_keyboard_for_chat(user.id, chat.type))

# === 🆘 /help
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    role = get_user_role(user.id)
    cmds = get_available_commands(user.id, chat.type)

    command_desc = {
        "start": "👋 Привітання і роль",
        "checkbot": "🟢 Перевірка статусу бота",
        "help": "📖 Це меню",
        "balance": "💰 Баланс",
        "pay": "📎 Завантажити платіж",
        "menu": "🔹 Меню дій"
    }

    text = f"📋 Доступні команди для ролі *{role}*:\n"
    for cmd in cmds:
        text += f"/{cmd} — {command_desc.get(cmd, '⚙️')}\n"

    logger.info(f"📖 /help від {user.id} ({user.username}) — роль: {role}, чат: {chat.type}")
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

    logger.info(f"✅ /checkbot від {user.id} ({user.username}) — {uptime_str}, чат: {chat.type}")
    await update.message.reply_text(f"✅ Бот онлайн\n⏱ Аптайм: {uptime_str}")

# === 💰 /balance (оновлено)
async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    role = get_user_role(user.id)

    if chat.type != "private" or role != "admin":
        logger.warning(f"⛔ /balance — доступ заборонено для {user.id} ({user.username})")
        return

    today = datetime.now().strftime("%d-%m-%Y")
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    pb_total = 0.0
    pb_result = "🏦 Безготівкові рахунки:\n"

    for name, token in PB_TOKENS.items():
        for acc in PB_ACCOUNTS.get(name, []):
            try:
                url = "https://acp.privatbank.ua/api/statements/balance"
                headers = {
                    "User-Agent": "PythonClient",
                    "token": token,
                    "Content-Type": "application/json;charset=cp1251"
                }
                params = {
                    "acc": acc,
                    "startDate": today,
                    "endDate": today
                }
                r = requests.get(url, headers=headers, params=params)
                data = r.json()

                for bal in data.get("balances", []):
                    balance = float(bal.get("balanceOutEq", 0))
                    if balance:
                        pb_total += balance
                        pb_result += f"- {bal.get('nameACC', name)}: {balance:,.2f} грн\n"
            except Exception as e:
                logger.error(f"💥 ПриватБанк {name} ({acc}): {e}")

    odata_total = 0.0
    odata_result = "\n💵 Готівкові рахунки:\n"

    for name, key in ODATA_ACCOUNTS.items():
        try:
            url = f"{ODATA_URL}AccumulationRegister_ДенежныеСредства/Balance?Period=datetime'{now_iso}'&$format=json&Condition=ДенежныйСчет_Key eq guid'{key}'"
            r = requests.get(url, auth=(ODATA_USER, ODATA_PASSWORD))
            r.raise_for_status()
            data = r.json()
            rows = data.get("value", [])
            if rows:
                amount = float(rows[0].get("СуммаBalance", 0))
                if amount:
                    odata_total += amount
                    odata_result += f"- {name}: {amount:,.2f} грн\n"
        except Exception as e:
            logger.error(f"💥 OData {name}: {e}")

    total = pb_total + odata_total
    summary = f"\n📊 Разом:\n- Безготівкові: {pb_total:,.2f} грн\n- Готівкові: {odata_total:,.2f} грн\n- 💰 Всього: {total:,.2f} грн"

    msg = f"{pb_result}{odata_result}{summary}"
    logger.info(f"💰 /balance — від {user.id} ({user.username}) — сума: {total:,.2f} грн")
    await update.message.reply_text(msg)

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


# === 🧾 Префікс для обробки дубліката файлу ===
CONFIRM_PREFIX = "confirm_duplicate_"

# === 📎 Обробка файлів /pay або /оплата ===
async def handle_payment_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 👤 Дані користувача
    user = update.effective_user
    chat = update.effective_chat
    message = update.effective_message
    username = user.username or "unknown"

    # 🔐 Перевірка ролі
    role = get_user_role(user.id)
    if role not in {"admin", "manager"}:
        logger.warning(f"⛔ Недостатньо прав: {user.id} ({username})")
        return

    # 📌 Перевірка чи є команда /pay або /оплата (в caption або reply)
    is_triggered = False
    if message.caption:
        is_triggered = any(x in message.caption.lower() for x in ["/pay", "/оплата"])
    if message.reply_to_message and message.text and any(x in message.text.lower() for x in ["/pay", "/оплата"]):
        is_triggered = True
        message = message.reply_to_message

    if not is_triggered or not message.document:
        logger.info(f"ℹ️ Пропуск: {user.id} ({username}) — без тригеру або без файлу")
        return

    # 📂 Перевірка дозволеного розширення
    file = message.document
    original_filename = file.file_name
    ext = os.path.splitext(original_filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        reply = "⚠️ Для оплати передайте файл у форматі: PDF, Excel, TXT, PNG, JPEG"
        await update.message.reply_text(reply)
        logger.warning(f"⚠️ Непідтримуваний формат: {original_filename}")
        return

    # 🧾 Перевірка в БД по назві файлу
    conn = get_db_connection()
    with conn.cursor() as cursor:
        sql = "SELECT * FROM telegram_files WHERE file_name = %s ORDER BY created_at DESC LIMIT 1"
        cursor.execute(sql, (original_filename,))
        existing = cursor.fetchone()
    conn.close()

    # 🟡 Якщо файл уже надсилався раніше — показуємо підтвердження
    if existing:
        text = (
            f"⚠️ Файл з такою назвою вже надсилався {existing['created_at'].strftime('%Y-%m-%d %H:%M')} "
            f"користувачем @{existing['username']}"
        )
        if existing['status'] == 'paid':
            text += f"\n✅ Оплачено: {existing['updated_at'].strftime('%Y-%m-%d %H:%M')}"
        text += "\n\nВідправити повторно на оплату?"

        unique_id = f"{chat.id}_{message.message_id}"
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Так", callback_data=CONFIRM_PREFIX + unique_id),
                InlineKeyboardButton("❌ Ні", callback_data="cancel")
            ]
        ])

        context.user_data[unique_id] = {
            "file": file,
            "file_name": original_filename,
            "message_id": message.message_id,
            "chat_id": chat.id,
            "username": username
        }

        await update.message.reply_text(text, reply_markup=keyboard)
        return

    # 🟢 Якщо дубліката нема — одразу зберігаємо
    await save_file_and_record(file, original_filename, chat.id, message.message_id, username, context)
    await update.message.reply_text("✅ Прийнято до сплати. Очікуйте повідомлення про оплату.")


# === ✅ Обробка callback Так / Ні (для дубліката) ===
async def confirm_duplicate_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # ❌ Відмова користувача
    if data == "cancel":
        await query.edit_message_text("🚫 Надсилання файлу на оплату скасовано.")
        return

    # 🔐 Перевірка на валідний формат callback_data
    if not data.startswith(CONFIRM_PREFIX):
        return

    unique_id = data.replace(CONFIRM_PREFIX, "")
    info = context.user_data.get(unique_id)
    if not info:
        await query.edit_message_text("⚠️ Дані для обробки не знайдено.")
        return

    file = info["file"]
    original_filename = info["file_name"]
    chat_id = info["chat_id"]
    message_id = info["message_id"]
    username = info["username"]

    # 📝 Створюємо нову назву для копії файлу
    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base, ext = os.path.splitext(original_filename)
    save_name = f"{base}_copy_{now_str}{ext}"

    # 💾 Зберігаємо файл з новою назвою
    await save_file_and_record(file, original_filename, chat_id, message_id, username, context, save_as=save_name)

    # ✅ Відповідь після підтвердження
    await query.edit_message_text(
        f"✅ Відправлено повторно з новою назвою: {save_name}"
    )


# === 💾 Збереження файлу та запис в БД ===
async def save_file_and_record(file, original_filename, chat_id, message_id, username, context, save_as=None):
    os.makedirs(SAVE_DIR, exist_ok=True)
    save_name = save_as or original_filename
    file_path = os.path.join(SAVE_DIR, save_name)

    # ⬇️ Завантаження з Telegram
    tg_file = await context.bot.get_file(file.file_id)
    await tg_file.download_to_drive(file_path)
    logger.info(f"📥 Збережено файл: {file_path}")

    # 🧮 Запис у таблицю telegram_files
    conn = get_db_connection()
    with conn.cursor() as cursor:
        sql = """
        INSERT INTO telegram_files (file_name, file_path, chat_id, message_id, username, timestamp, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), 'pending', NOW(), NOW())
        """
        cursor.execute(sql, (
            original_filename,
            file_path,
            chat_id,
            message_id,
            username
        ))
    conn.commit()
    conn.close()

# === 🧾 Лог усiх
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

# === 🚀 MAIN ===
def main():
    logger.info("🚀 Запуск бота...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # === 📌 Реєстрація команд ===
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("checkbot", checkbot_command))
    app.add_handler(CommandHandler("balance", balance_command))

    # === 📎 Обробка документів із /pay або /оплата
    app.add_handler(MessageHandler(
        filters.Document.ALL & (filters.CaptionRegex(r"(?i)/pay|/оплата") | filters.REPLY),
        handle_payment_file
    ))

    # === ✅ Обробка підтвердження дубліката через кнопки
    app.add_handler(CallbackQueryHandler(confirm_duplicate_handler))

    # === 🧾 Логування всіх повідомлень
    app.add_handler(MessageHandler(filters.ALL, log_everything))

    # === ❌ Глобальний обробник помилок
    app.add_error_handler(error_handler)

    try:
        app.run_polling()
    except Exception as e:
        logger.critical(f"🔥 Бот аварійно зупинився: {e}")


# === ▶️ Точка входу ===
if __name__ == "__main__":
    main()