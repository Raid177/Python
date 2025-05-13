import os
import logging
import requests
import schedule
import asyncio
import time
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

# === Завантаження змінних ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
FALLBACK_CHAT_ID = int(os.getenv("FALLBACK_CHAT_ID"))
ALLOWED_USERS_RAW = os.getenv("ALLOWED_USERS", str(FALLBACK_CHAT_ID))
ADMIN_USER = int(os.getenv("ADMIN_USER", FALLBACK_CHAT_ID))

if ALLOWED_USERS_RAW == "*":
    ALLOWED_USERS = "*"
else:
    ALLOWED_USERS = set(map(int, ALLOWED_USERS_RAW.split(',')))

# === Автоматичне збирання всіх ФОПів і рахунків ===
API_URL = "https://acp.privatbank.ua/api/statements/balance"
accounts = {}

for var in os.environ:
    if var.startswith("API_TOKEN_"):
        fop = var.replace("API_TOKEN_", "")
        token = os.getenv(var)
        acc_var = f"API_АСС_{fop}"
        acc_list = os.getenv(acc_var, "").split(",")
        acc_list = [acc.strip() for acc in acc_list if acc.strip()]
        accounts[fop] = [{"acc": acc, "token": token} for acc in acc_list]

# === Логування ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Отримання балансу ===
def get_today_balance(account, token):
    today = datetime.now().date()
    headers = {
        "User-Agent": "PythonClient",
        "token": token,
        "Content-Type": "application/json;charset=cp1251"
    }
    params = {
        "acc": account,
        "startDate": today.strftime("%d-%m-%Y"),
        "endDate": today.strftime("%d-%m-%Y")
    }

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

# === Форматування числа ===
def format_amount(value):
    try:
        parts = f"{value:,.2f}".split(".")
        int_part = parts[0].replace(",", "\u00a0")
        return f"{int_part},{parts[1]}"
    except:
        return str(value)

# === Побудова повідомлення ===
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
                    line = (
                        f"*{fop}*\n"
                        f"💳 `{bal['acc']}`\n"
                        f"💰 *{formatted} {bal['currency']}*"
                    )
                    report_lines.append(line)
                    if bal["currency"] == "UAH":
                        total_uah += value
                except Exception as e:
                    report_lines.append(f"*{fop}*: ⚠️ Помилка при обробці: {e}")

    if total_uah:
        total_formatted = format_amount(total_uah)
        report_lines.append(f"\n📊 *Загальна сума (UAH)*: *{total_formatted} грн*")

    return "\n\n".join(report_lines)

# === Доступ ===
def is_allowed(user_id):
    return ALLOWED_USERS == "*" or user_id in ALLOWED_USERS

# === Команди ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid) and uid != ADMIN_USER:
        return await update.message.reply_text("⛔ У вас немає доступу до цього бота.")

    kb = [["/pay"]]
    if uid == ADMIN_USER:
        kb[0].append("/balance")

    markup = ReplyKeyboardMarkup(kb, resize_keyboard=True)
    await update.message.reply_text("👋 Вітаю! Обери команду:", reply_markup=markup)

async def pay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid) and uid != ADMIN_USER:
        return await update.message.reply_text("⛔ У вас немає доступу.")
    await update.message.reply_text("📥 Тут буде список рахунків на оплату.")

async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != ADMIN_USER:
        return await update.message.reply_text("⛔ Доступ лише для адміністратора.")
    msg = build_balance_report()
    await update.message.reply_text(msg, parse_mode="Markdown")

# === Ключові слова (оплата) ===
async def handle_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.lower()
    if "оплата" in text:
        return await pay(update, context)

# === Розсилка ===
async def send_daily_report(application):
    msg = build_balance_report()
    try:
        await application.bot.send_message(chat_id=ADMIN_USER, text=msg, parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"❗ Не вдалося надіслати повідомлення {ADMIN_USER}: {e}")

# === Планувальник ===
def run_scheduler(application):
    schedule.every().day.at("10:00").do(lambda: asyncio.run(send_daily_report(application)))
    while True:
        schedule.run_pending()
        time.sleep(1)

# === Lock-файл ===
def ensure_single_instance(lockfile_path="balance_bot.lock"):
    if os.path.exists(lockfile_path):
        print("⚠️ Бот уже запущений. Вихід.")
        exit(1)
    with open(lockfile_path, "w") as f:
        f.write(str(os.getpid()))
    return lockfile_path

def cleanup_lock(lockfile_path):
    if os.path.exists(lockfile_path):
        os.remove(lockfile_path)

# === Старт ===
if __name__ == '__main__':
    lockfile = ensure_single_instance()
    try:
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("pay", pay))
        app.add_handler(CommandHandler("balance", balance))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_keywords))

        import threading
        threading.Thread(target=run_scheduler, args=(app,), daemon=True).start()

        app.run_polling()
    finally:
        cleanup_lock(lockfile)