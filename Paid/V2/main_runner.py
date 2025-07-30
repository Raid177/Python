import os
import tkinter as tk
from tkinter import filedialog
from file_handler import process_paid_file
from telegram_notify import send_payment_notification
from log import log
import subprocess
import winreg

def get_default_program(ext: str):
    try:
        with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, ext) as key:
            file_type, _ = winreg.QueryValueEx(key, None)

        cmd_path = fr"{file_type}\shell\open\command"
        with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, cmd_path) as key:
            command, _ = winreg.QueryValueEx(key, None)

        exe_path = command.split('"')[1] if '"' in command else command.split(" ")[0]
        return exe_path
    except Exception as e:
        log(f"❌ get_default_program({ext}): {e}")
        return None

def open_and_wait(file_path: str):
    ext = os.path.splitext(file_path)[1].lower()
    exe_path = get_default_program(ext)

    if not exe_path:
        log(f"⚠️ Програма за замовчуванням для {ext} не знайдена. Відкриваю через os.startfile.")
        try:
            os.startfile(file_path)
            input("➡️ Натисніть Enter після перегляду...")
        except Exception as e:
            log(f"❌ os.startfile({file_path}): {e}")
        return

    try:
        log(f"📂 Відкриття {file_path} через {exe_path}")
        proc = subprocess.Popen([exe_path, file_path])
        proc.wait()
        log(f"✅ Закрито: {file_path}")
    except Exception as e:
        log(f"❌ subprocess.Popen для {file_path}: {e}")

def ask_payment_gui(file_name, index, total):
    response = {"answer": None}

    def on_yes():
        log(f"🟢 Натиснуто ТАК для {file_name}")
        response["answer"] = "так"
        win.quit()
        win.destroy()

    def on_no():
        log(f"🟡 Натиснуто НІ для {file_name}")
        response["answer"] = "ні"
        win.quit()
        win.destroy()

    def on_exit():
        log(f"🔴 Натиснуто ВИЙТИ для {file_name}")
        response["answer"] = "вийти"
        win.quit()
        win.destroy()

    log(f"🪟 Відображення вікна підтвердження оплати для {file_name}")
    win = tk.Tk()
    win.title("Підтвердження оплати")
    win.attributes("-topmost", True)

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
    log(f"🧾 Вікно закрите, відповідь: {response['answer']}")
    return response["answer"]

def main():
    log("🚀 Старт програми")

    try:
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
            log("❌ Користувач не вибрав жодного файлу.")
            return

        for idx, file_path in enumerate(file_paths):
            file_name = os.path.basename(file_path)
            log(f"🗂️ Обробка файлу {idx + 1} з {len(file_paths)}: {file_name}")

            try:
                open_and_wait(file_path)
            except Exception as e:
                log(f"❌ open_and_wait() для {file_name}: {e}")
                continue

            try:
                resp = ask_payment_gui(file_name, idx, len(file_paths))
                log(f"🔘 Відповідь користувача: {resp}")
            except Exception as e:
                log(f"❌ ask_payment_gui() для {file_name}: {e}")
                continue

            if resp == "вийти":
                log("🚪 Завершення обробки користувачем.")
                break
            elif resp == "ні":
                log(f"⏭️ Пропущено: {file_name}")
                continue
            elif resp == "так":
                log(f"📥 Підтверджено оплату: {file_name}")
                try:
                    log("🛠 Запуск process_paid_file()...")
                    process_paid_file(file_path)
                    log("📤 Запуск send_payment_notification()...")
                    send_payment_notification(file_name)
                    log(f"✅ Завершено: {file_name}")
                except Exception as e:
                    log(f"❌ Помилка обробки {file_name}: {e}")
    except Exception as e:
        log(f"❌ Критична помилка: {e}")

    log("🏁 Програма завершена")

if __name__ == "__main__":
    main()
