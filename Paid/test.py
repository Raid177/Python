# Це бойова версія 1.2 Працює лише на сервері (спробуємо додати розпізнавання картинок з телефону та логування АПІ відповідей)
import os
import logging
from datetime import datetime
from dotenv import dotenv_values
from telegram import (
    Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
import requests
import pymysql
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.constants import ParseMode
from telegram import BotCommandScopeDefault

from telegram import BotCommand


#Меню команд
async def set_bot_commands(app):
    commands = [
        BotCommand("start", "показати роль"),
        BotCommand("checkbot", "статус бота"),
        BotCommand("help", "доступні команди"),
        BotCommand("about", "інформація про бота"),
        BotCommand("balance", "залишки по рахунках"),
        BotCommand("pay", "завантажити рахунок на оплату"),
        BotCommand("pending", "очікують оплати"),
    ]
    await app.bot.set_my_commands(commands)

# === 🔐 Конфігурація ===
env = dotenv_values("/root/Python/.env")
BOT_TOKEN = env["BOT_TOKEN"]
FALLBACK_CHAT_ID = int(env["FALLBACK_CHAT_ID"])
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "log.txt")


PB_TOKENS = {
    "LOV": env.get("API_TOKEN_LOV"),
    "ZVO": env.get("API_TOKEN_ZVO")
}
PB_ACCOUNTS = {
    "LOV": [env.get("API_АСС_LOV", "")],
    "ZVO": env.get("API_АСС_ZVO", "").split(",")
}

ODATA_URL = env.get("ODATA_URL")
ODATA_USER = env.get("ODATA_USER")
ODATA_PASSWORD = env.get("ODATA_PASSWORD")
ODATA_ACCOUNTS = {
    "Інкассація (транзитний)": "7e87f26e-eaad-11ef-9d9b-2ae983d8a0f0",
    "Реєстратура каса": "a7dda748-86d1-11ef-839c-2ae983d8a0f0",
    "Каса Організації": "f179f3be-4e84-11ef-83bb-2ae983d8a0f0"
}

SAVE_DIR = env.get("SAVE_DIR", "/root/Automation/Paid")
CONFIRM_PREFIX = "confirm_duplicate_"
ALLOWED_EXTENSIONS = {'.pdf', '.xls', '.xlsx', '.txt', '.jpeg', '.jpg', '.png'}

# === 📜 Логування ===
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# === 👥 Ролі з .env ===
ROLE_ADMIN = list(map(int, env.get("ROLE_ADMIN", "").split(",")))
ROLE_MANAGER = list(map(int, env.get("ROLE_MANAGER", "").split(",")))

# === 🕒 Час запуску ===
start_time = datetime.now()

# === 🎯 Визначення ролі користувача ===
def get_user_role(user_id: int) -> str:
    if user_id in ROLE_ADMIN:
        return "admin"
    elif user_id in ROLE_MANAGER:
        return "manager"
    return "employee"

# === 🔐 Перевірка дозволу ===
async def check_permission(update: Update, allowed_roles: set[str], private_only=False, group_only=False) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    role = get_user_role(user.id)

    if private_only and chat.type != "private":
        await update.message.reply_text("⚠️ Ця команда доступна лише в особистому чаті.")
        logger.warning(f"⚠️ {user.id} ({user.username}) — команда лише в приваті")
        return False

    if group_only and chat.type == "private":
        await update.message.reply_text("⚠️ Ця команда доступна лише в груповому чаті.")
        logger.warning(f"⚠️ {user.id} ({user.username}) — команда лише в групі")
        return False

    if role not in allowed_roles:
        await update.message.reply_text("⛔️ У вас немає прав для цієї дії.")
        logger.warning(f"⛔️ Недостатньо прав: {user.id} ({user.username})")
        return False

    return True


# === 💾 З'єднання з БД ===
def get_db_connection():
    return pymysql.connect(
        host=env["DB_HOST"],
        user=env["DB_USER"],
        password=env["DB_PASSWORD"],
        database=env["DB_DATABASE"],
        cursorclass=pymysql.cursors.DictCursor
    )

# === 📎 /pending ===
async def pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_permission(update, {"admin", "manager"}):
        return

    logger.info("🔍 Обробка /pending...")
    conn = get_db_connection()
    with conn.cursor() as cursor:
        logger.info("📥 SQL: Вибірка pending файлів")
        cursor.execute("""
            SELECT id, file_name, chat_id, message_id, created_at
            FROM telegram_files
            WHERE status = 'pending'
            ORDER BY created_at ASC
        """)
        rows = cursor.fetchall()
    conn.close()
    logger.info(f"📄 Знайдено {len(rows)} запис(ів) зі статусом 'pending'")

    if not rows:
        await update.message.reply_text("✅ Немає файлів, що очікують оплату.")
        return

    for row in rows:
        try:
            logger.info(f"📨 Обробка: {row['file_name']} (ID: {row['id']}, Chat: {row['chat_id']}, MsgID: {row['message_id']})")
            text = (
                f"📎 Очікує оплати: *{row['file_name']}*\n"
                f"🕒 {row['created_at'].strftime('%Y-%m-%d %H:%M')}"
            )
            await context.bot.send_message(
                chat_id=row["chat_id"],
                text=text,
                reply_to_message_id=row["message_id"],
                parse_mode=ParseMode.MARKDOWN
            )
        except (BadRequest, Forbidden) as e:
            logger.warning(f"❌ Повідомлення недоступне: {e}")
            await update.message.reply_text(
                f"❗ Неможливо відповісти на *{row['file_name']}*\n"
                f"📭 Ймовірно, повідомлення було видалено або бот не має доступу до чату.",
                parse_mode=ParseMode.MARKDOWN
            )
            try:
                logger.info(f"📝 Спроба оновити статус на 'invalid' для ID {row['id']}")
                conn = get_db_connection()
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE telegram_files SET status = 'invalid', updated_at = NOW() WHERE id = %s",
                        (row["id"],)
                    )
                conn.commit()
                conn.close()
                logger.info("✅ Статус оновлено успішно")
            except Exception as db_err:
                logger.error(f"⚠️ Помилка при оновленні статусу: {db_err}")

# === 📋 Команди за ролями ===
ROLE_COMMANDS = {
    "admin":    ["start", "checkbot", "help", "balance", "pay", "menu", "pending"],
    "manager":  ["start", "checkbot", "help", "balance", "pending"],
    "employee": ["start", "checkbot", "help"]
}

# === ⏱️ Доступнi команди ===
def get_available_commands(user_id: int, chat_type: str) -> list[str]:
    role = get_user_role(user_id)
    all_cmds = ROLE_COMMANDS.get(role, [])
    group_cmds = ["start", "checkbot", "help"]
    return [cmd for cmd in all_cmds if chat_type == "private" or cmd in group_cmds]

# # === 🧮 Клавiатура ===
# def get_keyboard_for_chat(user_id: int, chat_type: str):
#     commands = get_available_commands(user_id, chat_type)
#     buttons = [[f"/{cmd}" for cmd in commands[i:i+2]] for i in range(0, len(commands), 2)]
#     return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# === 🟢 /start ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    role = get_user_role(user.id)
    cmds = get_available_commands(user.id, chat.type)

    logger.info(f"➡️ /start від {user.id} ({user.username}) — роль: {role}, чат: {chat.type}")
    msg = f"👋 Привіт, {user.first_name}!\nВаша роль: *{role}*\n\n📋 Доступні команди:\n"
    msg += "\n".join(f"/{cmd}" for cmd in cmds)

    await update.message.reply_text(msg, parse_mode="Markdown")



# === 🆘 /help ===
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    role = get_user_role(user.id)
    cmds = get_available_commands(user.id, chat.type)

    command_desc = {
    "start": "👋 Привітання і роль",
    "checkbot": "🟢 Перевірка статусу бота",
    "help": "📖 Це меню",
    "balance": "💰 Баланс (лише в приваті)",
    "pay": "📎 Завантажити платіж (лише в групі)",
    "menu": "🔹 Меню дій",
    "pending": "📋 Перевірка неоплачених рахунків (лише в групі)"
}


    text = f"📋 Доступні команди для ролі *{role}*:\n"
    for cmd in cmds:
        text += f"/{cmd} — {command_desc.get(cmd, '⚙️')}\n"

    logger.info(f"📖 /help від {user.id} ({user.username}) — роль: {role}, чат: {chat.type}")
    await update.message.reply_text(text, parse_mode="Markdown")


# === ✅ /checkbot ===
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


# === 💰 /balance ===
async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_permission(update, {"admin"}, private_only=True):
        return

    user = update.effective_user
    logger.info(f"💰 /balance запит від {user.id} ({user.username})")

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
    await update.message.reply_text(msg)

# === 📎 Обробка файлів /pay або /оплата ===
async def handle_payment_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_permission(update, {"admin", "manager"}, group_only=True):
        return

    user = update.effective_user
    chat = update.effective_chat
    message = update.effective_message
    username = user.username or "unknown"

    is_triggered = False
    original_message = message
    file = None

    # Випадок 1: caption містить тригер
    if message.caption and any(x in message.caption.lower() for x in ["/pay", "/оплата"]):
        is_triggered = True
        file = message.document

    # Випадок 2: текст містить тригер, і є reply_to_message
    elif message.reply_to_message:
        reply = message.reply_to_message
        logger.info(f"📨 reply_to_message: {reply}")
        # Тригер у тексті повідомлення
        if message.text and any(x in message.text.lower() for x in ["/pay", "/оплата"]):
            # Файл у reply
            if reply.document:
                is_triggered = True
                file = reply.document
                original_message = reply
            elif reply.photo:
                is_triggered = True
                file = reply
                original_message = reply
        # Тригер у caption reply (на всякий випадок)
        elif reply.caption and any(x in reply.caption.lower() for x in ["/pay", "/оплата"]):
            if reply.document:
                is_triggered = True
                file = reply.document
                original_message = reply
            elif reply.photo:
                is_triggered = True
                file = reply
                original_message = reply


    logger.info(f"📎 Тригер: {is_triggered}, файл: {file.file_name if hasattr(file, 'file_name') else 'None'}")

    if not is_triggered or not file or not hasattr(file, 'file_id'):
        logger.info(f"ℹ️ Пропуск: {user.id} ({username}) — без тригеру або без файлу")
        return

    original_filename = getattr(file, 'file_name', None)
    if not original_filename:
        await update.message.reply_text("⚠️ Файл повинен мати назву. Неможливо обробити.")
        logger.warning("⚠️ Відсутнє ім'я файлу")
        return

    ext = os.path.splitext(original_filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        await update.message.reply_text("⚠️ Для оплати передайте файл у форматі: PDF, Excel, TXT, PNG, JPEG")
        logger.warning(f"⚠️ Непідтримуваний формат: {original_filename}")
        return

    conn = get_db_connection()
    with conn.cursor() as cursor:
        sql = "SELECT * FROM telegram_files WHERE file_name = %s ORDER BY created_at DESC LIMIT 1"
        cursor.execute(sql, (original_filename,))
        existing = cursor.fetchone()
    conn.close()

    if existing:
        text = (
            f"⚠️ Файл з такою назвою вже надсилався {existing['created_at'].strftime('%Y-%m-%d %H:%M')} "
            f"користувачем @{existing['username']}"
        )
        if existing['status'] == 'paid':
            text += f"\n✅ Оплачено: {existing['updated_at'].strftime('%Y-%m-%d %H:%M')}"
        text += "\n\nВідправити повторно на оплату?"

        unique_id = f"{chat.id}_{original_message.message_id}"
        keyboard = InlineKeyboardMarkup([[ 
            InlineKeyboardButton("✅ Так", callback_data=CONFIRM_PREFIX + unique_id),
            InlineKeyboardButton("❌ Ні", callback_data="cancel")
        ]])

        context.user_data[unique_id] = {
            "file": file,
            "file_name": original_filename,
            "message_id": original_message.message_id,
            "chat_id": chat.id,
            "username": username
        }

        await update.message.reply_text(text, reply_markup=keyboard)
        return

    # 🟢 Якщо не дублікат — зберігаємо одразу
    await save_file_and_record(file, original_filename, chat.id, original_message.message_id, username, context)
    await update.message.reply_text("✅ Прийнято до сплати. Очікуйте повідомлення про оплату.")



# === ✅ Обробка кнопки Так / Ні ===
async def confirm_duplicate_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "cancel":
        await query.edit_message_text("🚫 Надсилання файлу на оплату скасовано.")
        return

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

    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base, ext = os.path.splitext(original_filename)
    save_name = f"{base}_copy_{now_str}{ext}"

    await save_file_and_record(file, original_filename, chat_id, message_id, username, context, save_as=save_name)
    await query.edit_message_text(f"✅ Відправлено повторно з новою назвою: {save_name}")

# === 🖼️ Обробка фото (як платіжка) ===
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not await check_permission(update, {"admin", "manager"}, group_only=True):
        return

    if message.caption and any(x in message.caption.lower() for x in ["/pay", "/оплата"]):
        photo = message.photo[-1]  # Найякісніше зображення
        file = await context.bot.get_file(photo.file_id)
        filename = f"photo_{photo.file_unique_id}.jpg"
        await save_file_and_record(photo, filename, message.chat.id, message.message_id, message.from_user.username, context)
        await message.reply_text("✅ Фото платіжки збережено і додано до обробки.")
    else:
        await message.reply_text("⚠️ Додайте /pay або /оплата в підпис до фото, щоб зареєструвати платіжку.")


async def save_file_and_record(file, original_filename, chat_id, message_id, username, context, save_as=None):
    os.makedirs(SAVE_DIR, exist_ok=True)
    save_name = save_as or original_filename
    file_path = os.path.join(SAVE_DIR, save_name)

    tg_file = await context.bot.get_file(file.file_id)
    await tg_file.download_to_drive(file_path)
    logger.info(f"📥 Збережено файл: {file_path}")

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

# === 🧾 Логування всіх повідомлень ===
async def log_everything(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text if update.message else "N/A"
    logger.info(f"[ALL] {user.id} ({user.username}): {text}")


# === ❌ Глобальна обробка помилок ===
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"❌ ПОМИЛКА: {context.error}")
    if update and hasattr(update, "message"):
        try:
            await update.message.reply_text("⚠️ Виникла внутрішня помилка.")
        except Exception:
            pass


# === 🚀 MAIN ===
def main():
    logger.info("🚀 Запуск бота...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    #Обробник команд
    app.post_init = set_bot_commands


    # 📌 Команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("checkbot", checkbot_command))
    app.add_handler(CommandHandler("balance", balance_command))
    app.add_handler(CommandHandler("pending", pending_command))

    # Обробка фото телефону
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))


    # 📎 Обробка файлів із тригерами /pay або /оплата
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r"(?i)/pay|/оплата"),
        handle_payment_file
    ))
    app.add_handler(MessageHandler(
        filters.Document.ALL & filters.CaptionRegex(r"(?i)/pay|/оплата"),
        handle_payment_file
    ))

    # ✅ Підтвердження дубліката (inline-кнопки)
    app.add_handler(CallbackQueryHandler(confirm_duplicate_handler))

    # 🧾 Логування всіх повідомлень
    app.add_handler(MessageHandler(filters.ALL, log_everything))

    # ❌ Обробник помилок
    app.add_error_handler(error_handler)

    try:
        app.run_polling()
    except Exception as e:
        logger.critical(f"🔥 Бот аварійно зупинився: {e}")

# ▶️ Запуск
if __name__ == "__main__":
    main()
