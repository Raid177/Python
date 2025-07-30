# main_runner.py
"""
Головний модуль:
- Запитує вибір файлів для обробки (PDF, TXT, JPG, PNG, XLSX, DOC).
- Послідовно відкриває файли в дефолтних програмах.
- Після закриття питає: файл оплачено?
- Якщо "Так":
    - Викликає модуль для обробки (додає штамп/переміщує в Оплачено)
    - Викликає telegram_notify для оновлення статусу в БД і повідомлення
"""

import os
import tkinter as tk
from tkinter import filedialog
import subprocess
import time
from utils import wait_for_file_close, ask_payment_confirmation
from file_handler import process_paid_file
from telegram_notify import send_payment_notification

# Типи файлів для обробки
SUPPORTED_EXTENSIONS = ('.pdf', '.txt', '.jpg', '.jpeg', '.png', '.xlsx', '.xls', '.doc', '.docx')

def select_files():
    root = tk.Tk()
    root.withdraw()
    file_paths = filedialog.askopenfilenames(
        title="Виберіть файли для перегляду та оплати",
        filetypes=[("Допустимі файли", SUPPORTED_EXTENSIONS)]
    )
    return list(file_paths)

def main():
    files = select_files()
    if not files:
        print("❌ Файли не вибрано")
        return

    for file_path in files:
        file_name = os.path.basename(file_path)
        folder = os.path.dirname(file_path)

        print(f"\n📂 Відкриваємо: {file_name}")
        try:
            proc = subprocess.Popen(['start', '', file_path], shell=True)
            wait_for_file_close(proc)
        except Exception as e:
            print(f"❌ Не вдалося відкрити файл: {e}")
            continue

        if ask_payment_confirmation(file_name):
            new_path = process_paid_file(file_path)
            send_payment_notification(file_name)
        else:
            print(f"⏭️ Пропущено: {file_name}")

if __name__ == "__main__":
    main()


 # 🔧 Тест PDF-файлу — закоментуй після перевірки
    # from file_handler import process_paid_file
    # process_paid_file(r"C:\Users\la\OneDrive\Рабочий стол\testt.pdf")
