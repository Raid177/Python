#!/usr/bin/env python3
"""
run_all.py

Запуск пайплайну підрахунку ЗП:
- без аргументів: повний запуск всіх скриптів
- <script.py>: лише цей скрипт
- from <script.py>: цей і всі наступні

Логи: консоль + ../main_log.txt (із таймстампами та тривалістю)
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
    "summary.py",
]

# === Шлях до лог-файлу ===
BASE_DIR = os.path.dirname(__file__)
LOG_FILE = os.path.join(BASE_DIR, "..", "main_log.txt")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def current_timestamp():
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def log_write(message: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def print_and_log(message: str):
    line = f"{current_timestamp()} {message}"
    print(line)
    log_write(line)

def print_help():
    help_text = """
run_all.py — запуск скриптів підрахунку ЗП.

- Без аргументів — повний запуск всіх скриптів по порядку.
- <назва_скрипта.py> — запуск лише цього скрипта.
- from <назва_скрипта.py> — запуск цього скрипта та всіх наступних.

Приклади:
    python run_all.py
    python run_all.py fill_flat_schedule.py
    python run_all.py from worktime.py
"""
    print(help_text)
    log_write(help_text)

def run_script(script: str) -> bool:
    """Запускає окремий скрипт, стрімить stdout у лог, повертає True/False за фактом успіху."""
    script_path = os.path.join(BASE_DIR, script)
    if not os.path.isfile(script_path):
        print_and_log(f"[WARN] Скрипт не знайдено: {script}")
        return False
    print ("-=-=-=-=-=-=-=-=-=-=-=-")
    print_and_log(f"\n[RUN] Виконую скрипт: {script}")
    log_write("-" * 60)
    start_time = time.time()

    # Використовуємо системне кодування, але безпечніше підстрахувати utf-8
    encoding = locale.getpreferredencoding(False) or "utf-8"

    process = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding=encoding,
        cwd=BASE_DIR,   # щоб відносні шляхи скриптів працювали очікувано
        env=os.environ.copy(),
    )

    # Стрімио stdout у реальному часі
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            log_line = f"{current_timestamp()} {line.rstrip()}"
            print(log_line)
            log_write(log_line)

    exit_code = process.wait()
    elapsed_time = round(time.time() - start_time, 2)
    print_and_log(f"[TIME] Час виконання скрипта {script}: {elapsed_time} сек.")

    if exit_code != 0:
        print_and_log(f"[ERROR] Помилка виконання: {script} (exit={exit_code}). Зупиняємо.")
        sys.exit(exit_code)
    return True

def main():
    # Лог стартової команди
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
            except ValueError:
                print_and_log(f"[WARN] Скрипт '{args[1]}' не знайдено у списку.")
                sys.exit(1)
            for script in SCRIPTS[start_index:]:
                run_script(script)
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
        print_and_log(f"[FINISH] Загальний час виконання: {total_time} сек.")

if __name__ == "__main__":
    main()
