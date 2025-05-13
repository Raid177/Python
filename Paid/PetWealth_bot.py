# === telegram_bot_payments.py ===
# Включає: обробку файлів з /оплата, БД, кнопки, баланс, моніторинг статусу, видалення

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
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CallbackQueryHandler,
    CommandHandler, ContextTypes, filters
)

from dotenv import dotenv_values
env = dotenv_values("/root/Python/.env")

BOT_TOKEN = env["BOT_TOKEN"]
FALLBACK_CHAT_ID = int(env["FALLBACK_CHAT_ID"])
ADMIN_USER = int(env.get("ADMIN_USER", FALLBACK_CHAT_ID))
ALLOWED_USERS = env.get("ALLOWED_USERS", str(FALLBACK_CHAT_ID))

if ALLOWED_USERS == "*":
    ALLOWED_USERS_SET = "*"
else:
    ALLOWED_USERS_SET = set(map(int, ALLOWED_USERS.split(",")))

DB_HOST = env["DB_HOST"]
DB_USER = env["DB_USER"]
DB_PASSWORD = env["DB_PASSWORD"]
DB_DATABASE = env["DB_DATABASE"]
SAVE_DIR = env.get("SAVE_DIR", "C:/Users/la/OneDrive/Рабочий стол/На оплату!")
DELETED_DIR = os.path.join(SAVE_DIR, "Deleted")
LOG_FILE = env.get("LOG_FILE", "C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/Paid/from_telegram_log.txt")

print("ENV BOT_TOKEN:", repr(env.get("BOT_TOKEN")))
print("SAVE_DIR:", env.get("SAVE_DIR"))
print("LOG_FILE:", env.get("LOG_FILE"))


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

# === ❌ Вимкнено кілер процесів ===
# def kill_duplicates():
#     time.sleep(2)
#     current_pid = os.getpid()
#     this_name = os.path.basename(sys.argv[0])
#     for p in psutil.process_iter(['pid', 'name', 'exe', 'cmdline']):
#         try:
#             cmdline = p.info.get('cmdline') or []
#             if isinstance(cmdline, list) and this_name in ' '.join(cmdline) and p.pid != current_pid:
#                 p.kill()
#                 print(f"🛑 Вбито дубльований процес {this_name} (PID {p.pid})")
#         except Exception as e:
#             print(f"⚠️ Не вдалося завершити PID {p.pid}: {e}")
# kill_duplicates()

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
        int_part = parts[0].replace(",", " ")
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
        log(f"🚫 Користувач {user} скасував дубль")

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
    # app.add_handler(DeletedMessageHandler(handle_deleted))  # Видалено — не підтримується у PTB
    log("🤖 Бот запущено...")

    async def post_start(app):
        asyncio.create_task(check_paid_loop(app))

    app.post_init = post_start
    app.run_polling()
