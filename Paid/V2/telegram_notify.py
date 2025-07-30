"""
telegram_notify.py
Модуль для надсилання повідомлень в Telegram після оплати файлу.

🔹 Працює з таблицею `telegram_files` (поля: file_name, file_path, message_id, status).
🔹 Пошук ведеться по file_path (унікальний), бо file_name може повторюватись.
🔹 Якщо знайдено — надсилає повідомлення з реплаєм у MAIN_CHAT_ID.
🔹 Якщо не знайдено — надсилає повідомлення у ту ж MAIN_CHAT_ID без реплаю.
🔹 Оновлює статус на 'paid' у БД по file_path.
"""

import os
import requests
from db import get_server_connection
from log import log

BOT_TOKEN = os.getenv("BOT_TOKEN_PAY")
MAIN_CHAT_ID = os.getenv("MAIN_CHAT_ID", "-1002544853552")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

def send_payment_notification(file_path: str):
    """Надсилає повідомлення в Telegram після підтвердження оплати."""
    conn = get_server_connection()
    try:
        with conn.cursor() as cur:
            log(f"🔍 Пошук у telegram_files: file_path = {file_path}")
            cur.execute("""
                SELECT file_name, message_id FROM telegram_files
                WHERE file_path = %s
                ORDER BY id DESC LIMIT 1
            """, (file_path,))
            result = cur.fetchone()

        if result:
            file_name, message_id = result
            log(f"📦 Знайдено: file_name={file_name}, message_id={message_id}")
            msg = f"✅ Файл «{file_name}» оплачено"

            response = requests.post(API_URL, data={
                "chat_id": MAIN_CHAT_ID,
                "text": msg,
                "reply_to_message_id": message_id
            })
            log(f"📤 Telegram: {response.status_code} {response.text.encode('utf-8').decode('unicode_escape')}")

            # Оновлюємо статус
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE telegram_files SET status='paid' WHERE file_path = %s
                """, (file_path,))
                log(f"📝 Статус оновлено: paid для file_path = {file_path}")

        else:
            msg = (
                f"✅ Файл оплачено\n"
                f"⚠️ Але файл не знайдено в базі.\n‼️ Надсилайте файли через бота, щоб бот міг відповісти на оригінал.\n"
                f"📄 Шлях: {file_path}"
            )
            response = requests.post(API_URL, data={
                "chat_id": MAIN_CHAT_ID,
                "text": msg
            })
            log(f"📤 Telegram (no-reply): {response.status_code} {response.text}")

    except Exception as e:
        log(f"❌ Помилка надсилання в Telegram: {e}")
    finally:
        conn.close()


# 🔧 Тест при запуску напряму
if __name__ == "__main__":
    test_path = "/root/Automation/Paid/testt.pdf"
    send_payment_notification(test_path)
