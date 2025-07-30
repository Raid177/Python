# main.py
"""
Головний скрипт:
1. Дозволяє користувачу вибрати кілька файлів (PDF, TXT, DOC(X), XLS(X), JPG/PNG).
2. По черзі відкриває кожен файл у дефолтному застосунку.
3. Після закриття — питає, чи файл оплачено:
   - якщо так — додає штамп, переміщує у "Оплачено", надсилає повідомлення у Telegram і оновлює БД.
"""

import os
import time
import tkinter as tk
from tkinter import filedialog
import subprocess

from file_handler import process_paid_file
from telegram_notify import send_payment_notification
from log import log

def main():
    root = tk.Tk()
    root.withdraw()

    file_paths = filedialog.askopenfilenames(
        title="Оберіть файли для оплати",
        filetypes=[
            ("Усі підтримувані файли", ("*.pdf", "*.txt", "*.doc", "*.docx", "*.xls", "*.xlsx", "*.jpg", "*.jpeg", "*.png")),
            ("PDF", "*.pdf"),
            ("Текстові файли", "*.txt"),
            ("Документи Word", ("*.doc", "*.docx")),
            ("Таблиці Excel", ("*.xls", "*.xlsx")),
            ("Зображення", ("*.jpg", "*.jpeg", "*.png")),
        ]
    )

    if not file_paths:
        print("❌ Файли не обрано.")
        return

    for file_path in file_paths:
        print(f"\n📂 Відкриття файлу: {file_path}")
        try:
            # Відкриваємо у дефолтному застосунку
            proc = subprocess.Popen([file_path], shell=True)
            proc.wait()
        except Exception as e:
            log(f"❌ Не вдалося відкрити файл: {file_path} — {e}")
            continue

        # Запит: чи файл оплачено?
        while True:
            resp = input("💰 Чи файл оплачено? (так / ні / вийти): ").strip().lower()
            if resp in ("так", "ні", "вийти"):
                break

        if resp == "вийти":
            print("🚪 Завершення обробки.")
            break

        if resp == "ні":
            print("⏭️ Пропущено.")
            continue

        # Якщо файл оплачено:
        try:
            file_name = os.path.basename(file_path)
            process_paid_file(file_path)
            send_payment_notification(file_name)
        except Exception as e:
            log(f"❌ Помилка при обробці файлу {file_path}: {e}")

if __name__ == "__main__":
    main()
