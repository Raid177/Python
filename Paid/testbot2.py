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

# === 🚀 MAIN

def main():
    logger.info("🚀 Запуск бота...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("checkbot", checkbot_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("balance", balance_command))
    app.add_handler(MessageHandler(filters.ALL, log_everything))

    app.add_error_handler(error_handler)

    try:
        app.run_polling()
    except Exception as e:
        logger.critical(f"🔥 Бот аварійно зупинився: {e}")

if __name__ == "__main__":
    main()
