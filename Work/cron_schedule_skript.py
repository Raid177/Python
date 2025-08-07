#!/usr/bin/env python3
import time
import subprocess
import threading
from datetime import datetime
import mysql.connector
import re
import os
from dotenv import load_dotenv

load_dotenv("/root/Python/.env")

# === Конфіг БД ===
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

# === Шлях до скриптів ===
SCRIPTS_ROOT = "/root/Python"

# === Шаблони інтервалів ===
interval_matchers = [
    (r"^daily@(\d{2}:\d{2})$", lambda t, m: t.strftime("%H:%M") == m),
    (r"^weekly@([A-Za-z]{3}) (\d{2}:\d{2})$", lambda t, m1, m2: t.strftime("%a") == m1 and t.strftime("%H:%M") == m2),
    (r"^monthly@(\d{1,2}) (\d{2}:\d{2})$", lambda t, m1, m2: int(t.day) == int(m1) and t.strftime("%H:%M") == m2),
    (r"^hourly@(\d+)$", lambda t, m: t.minute == 0 and t.hour % int(m) == 0),
    (r"^every@(\d+)minutes$", lambda t, m: t.minute % int(m) == 0),
    (r"^every@(\d+)(days|weeks|months)$", None),  # ще не реалізовано
]

def should_run(interval, last_run):
    now = datetime.now()
    for pat, func in interval_matchers:
        m = re.match(pat, interval)
        if m and func:
            if func(now, *m.groups()):
                return True
    return False

def process_task(rule, scripts_root):
    folder, script, interval, run_after, parallel, comment, duplicate, errors = rule
    now = datetime.now()

    print(f"⏳ [{now.strftime('%H:%M:%S')}] Перевіряємо {folder}/{script}...")

    # Перевірка залежності
    if run_after:
        cur = conn.cursor()
        dep = run_after.split("/") if "/" in run_after else (None, run_after)
        sql = """SELECT status, end_time FROM cron_script_log
                 WHERE script_name=%s ORDER BY end_time DESC LIMIT 1"""
        cur.execute(sql, (dep[1],))
        row = cur.fetchone()
        if not row or row[0] != "ok":
            print(f"⛔ Пропущено {folder}/{script} — залежність {run_after} не завершена успішно.")
            return

    start = datetime.now()
    full_path = os.path.join(scripts_root, folder, script)

    try:
        proc = subprocess.Popen(
            ["/usr/bin/env", "python3", full_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        out, err = proc.communicate()
        status = "ok" if proc.returncode == 0 else "fail"
    except Exception as e:
        out = ""
        err = f"❌ Exception: {str(e)}"
        status = "fail"

    end = datetime.now()
    duration = int((end - start).total_seconds())

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cron_script_log 
        (script_name, script_path, start_time, end_time, duration_sec, created_at, status, log_output, stderr_output)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        script,
        full_path,
        start,
        end,
        duration,
        datetime.now(),
        status,
        out.strip(),
        err.strip()
    ))
    conn.commit()

    print(f"✅ [{end.strftime('%H:%M:%S')}] Виконано {folder}/{script} → {status}, {duration} сек.")

if __name__ == "__main__":
    conn = mysql.connector.connect(**DB_CONFIG)
    print("🚀 Запущено cron_schedule_skript")

    try:
        while True:
            cur = conn.cursor()
            cur.execute("SELECT folder, script_name, run_interval, run_after, parallel, comment, duplicate_of, errors FROM cron_script_rules")
            rules = cur.fetchall()

            for rule in rules:
                folder, script, interval, run_after, parallel, *rest = rule
                errors = rest[-1]

                if errors:
                    continue  # є помилки — не виконуємо
                if not interval:
                    continue  # порожній інтервал

                if should_run(interval, None):
                    print(f"➡️ Час запуску для {folder}/{script}")
                    if parallel.lower() == "так":
                        threading.Thread(target=process_task, args=(rule, SCRIPTS_ROOT)).start()
                    else:
                        process_task(rule, SCRIPTS_ROOT)

            time.sleep(60)
    finally:
        conn.close()
