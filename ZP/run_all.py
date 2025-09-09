#!/usr/bin/env python3
"""
run_all.py

Скрипт для послідовного або вибіркового запуску модулів підрахунку ЗП.

✅ Запускає скрипти у заданій послідовності або з аргументами:
   - без аргументів: повний запуск всіх скриптів
   - <script.py>: запуск лише цього скрипта
   - from <script.py>: запуск цього скрипта та всіх наступних.
✅ Виводить логи в консоль та записує їх у файл main_log.txt (у батьківській папці), з таймстампом.
✅ Додає резюме часу виконання кожного скрипта.
✅ Після виконання всіх скриптів — виводить загальний час виконання.
✅ Якщо скрипт вивалюється з помилкою авторизації Google, пропонує пройти авторизацію через authorize.py, а потім запуститися заново або завершитися.
"""

import subprocess
import sys
import os
import datetime
import time
import locale

# === Послідовність скриптів ===
SCRIPTS = [
    "import_enote_reference.py",
    "auto_fill_idx.py",
    "diagnose_sheets.py",
    "fill_flat_schedule.py",
    "worktime.py",
    "sync_fkt_UmoviOplaty.py",
    "sync_умови_рівень.py",
    "zp_sales_salary_new.py",
    "zp_collective_bonus.py",
    "summary.py"
]

# === Шлях до лог-файлу ===
LOG_FILE = os.path.join(os.path.dirname(__file__), "..", "main_log.txt")

def current_timestamp():
    """Отримати таймстамп у форматі [YYYY-MM-DD HH:MM:SS]."""
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def log_write(message):
    """Запис рядка у лог-файл."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def print_and_log(message):
    """Вивід у консоль та запис у лог з таймстампом."""
    line = f"{current_timestamp()} {message}"
    print(line)
    log_write(line)

def print_help():
    help_text = """
run_all.py — запуск скриптів підрахунку ЗП.

- Без аргументів — повний запуск всіх скриптів по порядку.
- <назва_скрипта.py> — запуск лише цього скрипта.
- from <назва_скрипта.py> — запуск цього скрипта та всіх наступних.

Наприклад:
    python run_all.py
    python run_all.py fill_flat_schedule.py
    python run_all.py from worktime.py
"""
    print(help_text)
    log_write(help_text)

def run_script(script):
    """Запускає окремий скрипт, логуючи stdout/stderr та перевіряючи помилки авторизації Google."""
    script_path = os.path.join(os.path.dirname(__file__), script)
    if not os.path.isfile(script_path):
        print_and_log(f"[WARN] Скрипт не знайдено: {script}")
        return False

    print_and_log(f"[RUN] Виконую скрипт: {script}")
    log_write("-" * 60)
    start_time = time.time()

    process = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding=locale.getpreferredencoding(False)  # Використання системного кодування
    )

    error_google_auth = False
    google_keywords = [
        "google authorization",
        "refresh token",
        "expired token",
        "refresherror",
        "invalidgrant",
        "invalid_scope",
        "bad request"
    ]

    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            log_line = f"{current_timestamp()} {line.rstrip()}"
            print(log_line)
            log_write(log_line)
            if any(keyword in line.lower() for keyword in google_keywords):
                error_google_auth = True

    exit_code = process.wait()
    elapsed_time = round(time.time() - start_time, 2)
    print_and_log(f"[TIME] Час виконання скрипта {script}: {elapsed_time} сек.")

    if exit_code != 0:
        if error_google_auth:
            print_and_log(f"[ERROR] Проблема з авторизацією Google у скрипті: {script}.")
            choice = input("Пройти авторизацію? (так/ні): ").strip().lower()
            if choice in ("так", "y", "yes"):
                run_script("authorize.py")
                print_and_log("[OK] Авторизація Google пройдена.")
                restart = input("Запустити все заново? (так/ні): ").strip().lower()
                if restart in ("так", "y", "yes"):
                    print_and_log("[SYNC] Повторний запуск з тими ж параметрами...")
                    python_exe = sys.executable
                    try:
                        subprocess.run([python_exe, sys.argv[0]] + sys.argv[1:], check=True)
                    except subprocess.CalledProcessError as e:
                        print_and_log(f"[ERROR] Повторний запуск завершився з помилкою: {e.returncode}")
                    sys.exit(0)
                else:
                    print_and_log("[WARN] Завершення роботи.")
                    sys.exit(1)
            else:
                print_and_log("[WARN] Завершення роботи.")
                sys.exit(1)
        else:
            print_and_log(f"[ERROR] Помилка виконання: {script}. Зупиняємо.")
            sys.exit(exit_code)
    return True

def main():
    # === Логування команди запуску ===
    command_line = " ".join(sys.argv)
    log_write("=" * 80)
    log_write(f"{current_timestamp()} Запущено з командою: {command_line}")
    log_write("=" * 80)

    start_all_time = time.time()

    args = sys.argv[1:]

    try:
        if not args:
            for script in SCRIPTS:
                run_script(script)
        elif args[0] in ("-h", "--help"):
            print_help()
        elif args[0] == "from" and len(args) == 2:
            try:
                start_index = SCRIPTS.index(args[1])
                for script in SCRIPTS[start_index:]:
                    run_script(script)
            except ValueError:
                print_and_log(f"[WARN] Скрипт '{args[1]}' не знайдено у списку.")
                sys.exit(1)
        elif len(args) == 1:
            if args[0] in SCRIPTS:
                run_script(args[0])
            else:
                print_and_log(f"[WARN] Скрипт '{args[0]}' не знайдено у списку.")
                sys.exit(1)
        else:
            print_and_log("[WARN] Невідомі аргументи.")
            print_help()
    finally:
        total_time = round(time.time() - start_all_time, 2)
        print_and_log(f"[FINISH] Загальний час виконання: {total_time} сетакк.")

if __name__ == "__main__":
    main()
