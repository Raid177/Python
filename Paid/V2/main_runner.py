# main.py
"""
Головний скрипт:
1. Дозволяє користувачу вибрати кілька файлів (PDF, TXT, DOC(X), XLS(X), JPG/PNG).
2. По черзі відкриває кожен файл у дефолтному застосунку.
3. Після закриття — питає, чи файл оплачено:
   - якщо так — додає штамп, переміщує у "Оплачено", надсилає повідомлення у Telegram і оновлює БД.
"""

import os
import tkinter as tk
from tkinter import filedialog
import subprocess

from file_handler import process_paid_file
from telegram_notify import send_payment_notification
from log import log

def ask_payment_gui(file_name, index, total):
    """Показує вікно з кнопками: Так / Ні / Вийти"""
    response = {"answer": None}

    def on_yes():
        response["answer"] = "так"
        win.destroy()

    def on_no():
        response["answer"] = "ні"
        win.destroy()

    def on_exit():
        response["answer"] = "вийти"
        win.destroy()

    win = tk.Tk()
    win.title("Підтвердження оплати")

    label = tk.Label(
        win,
        text=f"Файл {index + 1} з {total}:\n{file_name}\nОплачено?",
        font=("Arial", 12),
        pady=10,
        padx=20
    )
    label.pack()

    btn_frame = tk.Frame(win, pady=10)
    btn_frame.pack()

    tk.Button(btn_frame, text="Так", width=10, command=on_yes, bg="lightgreen").pack(side="left", padx=5)
    tk.Button(btn_frame, text="Ні", width=10, command=on_no, bg="lightyellow").pack(side="left", padx=5)
    tk.Button(btn_frame, text="Вийти", width=10, command=on_exit, bg="lightcoral").pack(side="left", padx=5)

    win.mainloop()
    return response["answer"]

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

    for idx, file_path in enumerate(file_paths):
        print(f"\n📂 Відкриття файлу: {file_path}")
        try:
            proc = subprocess.Popen([file_path], shell=True)
            proc.wait()
        except Exception as e:
            log(f"❌ Не вдалося відкрити файл: {file_path} — {e}")
            continue

        file_name = os.path.basename(file_path)
        resp = ask_payment_gui(file_name, idx, len(file_paths))

        if resp == "вийти":
            print("🚪 Завершення обробки.")
            break
        elif resp == "ні":
            print("⏭️ Пропущено.")
            continue
        elif resp == "так":
            try:
                process_paid_file(file_path)
                send_payment_notification(file_name)
            except Exception as e:
                log(f"❌ Помилка при обробці файлу {file_path}: {e}")

if __name__ == "__main__":
    main()
