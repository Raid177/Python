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


def get_latest_balance(account, token):
    today = datetime.now().date()
    headers = {"User-Agent": "PythonClient", "token": token, "Content-Type": "application/json;charset=cp1251"}

    def fetch(date):
        params = {"acc": account, "startDate": date.strftime("%d-%m-%Y"), "endDate": date.strftime("%d-%m-%Y")}
        try:
            response = requests.get(API_URL, headers=headers, params=params)
            if response.status_code != 200:
                return None, f"❌ {response.status_code}: {response.text}", None
            data = response.json()
            if data["status"] == "SUCCESS" and data.get("balances"):
                return data["balances"][0], None, datetime.combine(date, datetime.max.time().replace(microsecond=0))
        except Exception as e:
            return None, str(e), None
        return None, None, None

    bal, err, used = fetch(today)
    if bal: return bal, None, used
    yesterday = today - timedelta(days=1)
    bal, err, used = fetch(yesterday)
    if bal: return bal, None, used
    return None, err or "❌ Баланс не знайдено", None

def format_amount(value):
    try:
        parts = f"{value:,.2f}".split(".")
        int_part = parts[0].replace(",", "\xa0")  # не-розривний пробіл
        return f"{int_part},{parts[1]}"
    except:
        return str(value)


def build_balance_report():
    total_uah = 0
    lines = []
    for fop, acc_list in accounts.items():
        for acc_info in acc_list:
            bal, err, dt_used = get_latest_balance(acc_info["acc"], acc_info["token"])
            if err:
                lines.append(f"*{fop}*: {err}")
            elif bal:
                try:
                    value = float(bal["balanceOut"])
                    if value == 0:
                        continue
                    formatted = format_amount(value)
                    lines.append(f"*{fop}* (🕓 {dt_used.strftime('%d.%m.%Y %H:%M')})\n💳 `{bal['acc']}`\n💰 *{formatted} {bal['currency']}*")
                    if bal["currency"] == "UAH":
                        total_uah += value
                except Exception as e:
                    lines.append(f"*{fop}*: ⚠️ {e}")
    if total_uah:
        lines.append(f"\n📊 *Загальна сума (UAH)*: *{format_amount(total_uah)} грн*")
    return "\n\n".join(lines)


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

    # === Команда в caption
    caption = msg.caption.lower() if msg.caption else ""
    is_pay_command = "/оплата" in caption or "/pay" in caption

    # === Якщо команда не знайдена, але є reply на документ
    file_msg = None
    if not is_pay_command and msg.reply_to_message:
        replied = msg.reply_to_message
        rep_caption = replied.caption.lower() if replied.caption else ""
        if "/оплата" in rep_caption or "/pay" in rep_caption:
            is_pay_command = True
            file_msg = replied

    # === Основний файл — або сам документ, або з reply
    if is_pay_command:
        file_msg = file_msg or msg
        user = file_msg.from_user.username or "anon"

        if file_msg.document:
            file = file_msg.document
            orig_name = file.file_name
            cursor.execute("SELECT timestamp, status FROM telegram_files WHERE file_name=%s AND username=%s ORDER BY id DESC LIMIT 1", (orig_name, user))
            prev = cursor.fetchone()
            if prev:
                prev_time, prev_status = prev
                if prev_status == "paid":
                    await msg.reply_text("🔒 Цей файл вже оплачено. Повторне надсилання заборонено.")
                    return
                sessions[user] = (file, file_msg)
                keyboard = [
                    [InlineKeyboardButton("✅ Зберегти ще раз", callback_data="save_again"),
                     InlineKeyboardButton("❌ Скасувати", callback_data="cancel")]
                ]
                await msg.reply_text(f"⚠️ Файл «{orig_name}» вже надсилався {prev_time.strftime('%Y-%m-%d %H:%M:%S')}. Повторити збереження?", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await save_and_record(file, file_msg, context, user, is_duplicate=False)
        else:
            await msg.reply_text("🚫 Повідомлення не містить документа. Надішліть PDF, Excel, TXT або зображення як *документ*, а не фото/вкладення.", parse_mode="Markdown")

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
        log(f"🚫 Користувач {user} скасував дубль")

# === 📊 Команда /balance ===
async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    print(f"[DEBUG] balance від {uid}")
    if uid != ADMIN_USER:
        return await update.message.reply_text("⛔ Доступ лише для адміністратора.")
    msg = build_balance_report()
    print(f"[DEBUG] звіт балансу:\n{msg}")
    if not msg.strip():
        msg = "⚠️ Балансів не знайдено або відповідь була порожня."
    await update.message.reply_text(msg, parse_mode="Markdown")


# === 🚀 Старт ===
if __name__ == "__main__":
    from telegram import BotCommand, BotCommandScopeChat, BotCommandScopeDefault
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(CommandHandler("balance", balance))
    log("🤖 Бот запущено...")

    # === ✅ Об'єднаний post_start
    async def post_start(app):
        print("👉 post_start запущено")
        log("🚀 post_start запущено")

        # 🕙 Автобаланс
        scheduler = AsyncIOScheduler()

        async def send_daily_balance():
            msg = build_balance_report()
            if not msg.strip():
                msg = "⚠️ Балансів не знайдено або відповідь була порожня."
            try:
                await app.bot.send_message(chat_id=ADMIN_USER, text=msg, parse_mode="Markdown")
                log("📤 Автобаланс надіслано адміну")
            except Exception as e:
                log(f"⚠️ Помилка при надсиланні автобалансу: {e}")
                print(f"[ERROR] Автобаланс: {e}")

        scheduler.add_job(send_daily_balance, "cron", hour=10, minute=0)
        scheduler.start()

        # 🧠 Цикл перевірки оплат
        asyncio.create_task(check_paid_loop(app))

        # 📎 Команди
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
            print(f"[ERROR] Команди: {e}")

    app.post_init = post_start
    app.run_polling()
