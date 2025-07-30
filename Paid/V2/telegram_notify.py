"""
telegram_notify.py
Модуль для надсилання повідомлень в Telegram після оплати файлу.

- Працює з таблицею `telegram_files` (поля: file_path, message_id, status).
- Пошук ведеться по file_path.
- Якщо знаходить файл у БД — надсилає повідомлення з реплаєм у MAIN_CHAT_ID.
- Якщо не знаходить — надсилає повідомлення у MAIN_CHAT_ID без реплаю.
- Оновлює статус на 'paid' у БД.
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

            # 💬 Декодування Unicode escape:
            log(f"📤 Telegram: {response.status_code} {response.text.encode('utf-8').decode('unicode_escape')}")

            # Оновлюємо статус
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE telegram_files SET status='paid' WHERE file_path = %s
                """, (file_path,))
        else:
            # Файл не знайдено в БД
            file_name = os.path.basename(file_path)
            msg = (
                f"✅ Файл «{file_name}» оплачено\n"
                f"⚠️ Але файл не знайдено в базі.\n‼️ Надсилайте файли через бота, щоб бот міг відповісти на оригінал."
            )
            response = requests.post(API_URL, data={
                "chat_id": MAIN_CHAT_ID,
                "text": msg
            })
            log(f"📤 Telegram (no-reply): {response.status_code} {response.text.encode('utf-8').decode('unicode_escape')}")

    except Exception as e:
        log(f"❌ Помилка надсилання в Telegram: {e}")
    finally:
        conn.close()


# 🔧 Тест при запуску напряму
if __name__ == "__main__":
    # 🧪 Заміни шлях на актуальний
    send_payment_notification("/root/Automation/Paid/testt_copy_2025-07-30_16-19-20.pdf")
