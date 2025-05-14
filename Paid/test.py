# === telegram_bot_payments.py ===
# Telegram-бот для прийому файлів, моніторингу оплат і виводу балансів по рахунках ПриватБанку

import os
import sys
import time
import tempfile
import atexit
import pymysql
import psutil
import requests
import shutil
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv, dotenv_values
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CallbackQueryHandler,
    CommandHandler, ContextTypes, filters
)

# === 🌱 Змінні середовища ===
env = dotenv_values("/root/Python/.env")
BOT_TOKEN = env["BOT_TOKEN"]
FALLBACK_CHAT_ID = int(env["FALLBACK_CHAT_ID"])
ADMIN_USER = int(env.get("ADMIN_USER", FALLBACK_CHAT_ID))
ALLOWED_USERS = env.get("ALLOWED_USERS", str(FALLBACK_CHAT_ID))
ALLOWED_USERS_SET = "*" if ALLOWED_USERS == "*" else set(map(int, ALLOWED_USERS.split(",")))

DB_HOST = env["DB_HOST"]
DB_USER = env["DB_USER"]
DB_PASSWORD = env["DB_PASSWORD"]
DB_DATABASE = env["DB_DATABASE"]
SAVE_DIR = env.get("SAVE_DIR", "/root/Python/data/incoming")
DELETED_DIR = os.path.join(SAVE_DIR, "Deleted")
LOG_FILE = env.get("LOG_FILE", "/root/Python/data/logs/from_telegram_log.txt")

# === 📃 Підключення до БД ===
conn = pymysql.connect(
    host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE,
    charset='utf8mb4', autocommit=True
)
cursor = conn.cursor()
sessions = {}
payment_notified = set()

# === 💳 Баланси ПриватБанк ===
API_URL = "https://acp.privatbank.ua/api/statements/balance"
accounts = {}
for var in env:
    if var.startswith("API_TOKEN_"):
        fop = var.replace("API_TOKEN_", "")
        token = env[var]
        acc_list = env.get(f"API_АСС_{fop}", "").split(",")
        accounts[fop] = [{"acc": acc.strip(), "token": token} for acc in acc_list if acc.strip()]

# === 🫅 Логи ===
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# === 💾 Збереження файлу ===
async def save_and_record(file, file_msg, context, user, is_duplicate):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_name = file.file_name
    safe_name = f"{os.path.splitext(base_name)[0]}__DUPLICATE_{ts}{os.path.splitext(base_name)[1]}" if is_duplicate else base_name
    file_path = os.path.join(SAVE_DIR, safe_name)
    os.makedirs(SAVE_DIR, exist_ok=True)
    telegram_file = await context.bot.get_file(file.file_id)
    await telegram_file.download_to_drive(file_path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""
        INSERT INTO telegram_files (file_name, file_path, chat_id, message_id, username, timestamp, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'pending')
    """, (base_name, file_path, file_msg.chat.id, file_msg.message_id, user, now))
    note = " (дубль)" if is_duplicate else ""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑️ Видалити", callback_data=f"delete_file:{file_msg.message_id}")]
    ])
    await file_msg.reply_text(f"📥 Файл «{base_name}»{note} передано на оплату", reply_markup=keyboard)
    log(f"✔️ {base_name} збережено {note}")

# === 🔘 Обробка callback-кнопок ===
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user.username or "anon"

    if query.data == "save_again":
        if user not in sessions:
            await query.edit_message_text("⚠️ Сесію не знайдено")
            return
        file, msg = sessions.pop(user)
        await save_and_record(file, msg, context, user, is_duplicate=True)
        await query.edit_message_text("✅ Повторне збереження виконано")
        return

    if query.data.startswith("delete_file:"):
        msg_id = int(query.data.split(":")[1])
        chat_id = query.message.chat.id
        cursor.execute("SELECT id, file_name, file_path, status FROM telegram_files WHERE chat_id=%s AND message_id=%s", (chat_id, msg_id))
        row = cursor.fetchone()
        if row:
            fid, fname, fpath, status = row
            if status == 'paid':
                await query.edit_message_text(f"🔒 Файл *{fname}* вже оплачено. Видалення заборонено.", parse_mode="Markdown")
                return
            cursor.execute("UPDATE telegram_files SET status='deleted', deleted_by='user', deleted_at=NOW() WHERE id=%s", (fid,))
            os.makedirs(DELETED_DIR, exist_ok=True)
            try:
                shutil.move(fpath, os.path.join(DELETED_DIR, os.path.basename(fpath)))
                log(f"🗑️ Файл {fname} переміщено у Deleted")
            except Exception as e:
                log(f"⚠️ Не вдалося перемістити файл {fname}: {e}")
            await query.edit_message_text(f"🗑️ Файл *{fname}* видалено та переміщено у архів.", parse_mode="Markdown")
        else:
            await query.edit_message_text("⚠️ Файл не знайдено в базі")

# === 🔖 Реєстрація обробників ===
if __name__ == "__main__":
    from telegram import BotCommand, BotCommandScopeChat, BotCommandScopeDefault
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.ALL, handle_file))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(CommandHandler("balance", balance))
    log("🤖 Бот запущено...")

    async def post_start(app):
        log("🚀 post_start запущено")
        try:
            await app.bot.set_my_commands([
                BotCommand("pay", "Надіслати файл для оплати")
            ], scope=BotCommandScopeDefault())
            await app.bot.set_my_commands([
                BotCommand("pay", "Надіслати файл для оплати"),
                BotCommand("balance", "Залишки на рахунках")
            ], scope=BotCommandScopeChat(chat_id=ADMIN_USER))
            log("✅ Команди успішно встановлено")
        except Exception as e:
            log(f"⚠️ Помилка при встановленні команд: {e}")

    app.post_init = post_start
    app.run_polling()
