#!/usr/bin/env python3
import time, subprocess, threading
from datetime import datetime, timedelta
import mysql.connector
import re
import os
from dotenv import load_dotenv
import os

load_dotenv("/root/Python/.env")


DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

SCRIPTS_ROOT = "/root/Python"

interval_matchers = [
    (r"^daily@(\d{2}:\d{2})$", lambda t, m: t.strftime("%H:%M")==m),
    (r"^weekly@([A-Za-z]{3}) (\d{2}:\d{2})$", lambda t,m1,m2:t.strftime("%a")==m1 and t.strftime("%H:%M")==m2),
    (r"^monthly@(\d{1,2}) (\d{2}:\d{2})$", lambda t,m1,m2:int(t.day)==int(m1) and t.strftime("%H:%M")==m2),
    (r"^hourly@(\d+)$", lambda t,m: t.minute==0 and t.hour % int(m)==0),
    (r"^every@(\d+)(days|weeks|months)$", None),  # поки не обробляється
]

def should_run(interval, last_run):
    now = datetime.now()
    for pat, func in interval_matchers:
        m = re.match(pat, interval)
        if m and func:
            if func(now, *m.groups()):
                return True
    return False

def process_task(rule):
    folder, script, interval, run_after, parallel, comment, duplicate, errors = rule
    now = datetime.now()
    # Перевірка залежності
    if run_after:
        cur = conn.cursor()
        dep = run_after.split("/") if "/" in run_after else (None, run_after)
        sql = """SELECT status, end_time FROM cron_script_log
                 WHERE folder=%s AND script_name=%s
                 ORDER BY end_time DESC LIMIT 1"""
        cur.execute(sql, (dep[0] or "", dep[1]))
        row = cur.fetchone()
        if not row or row[0]!="ok":
            return
    start = datetime.now()
    proc = subprocess.Popen(
        [os.path.join(SCRIPTS_ROOT, folder, script)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    out, err = proc.communicate()
    status = "ok" if proc.returncode==0 else "fail"
    end = datetime.now()
    duration = int((end-start).total_seconds())
    cur = conn.cursor()
    cur.execute("""INSERT INTO cron_script_log
                   (script_name, start_time, end_time, duration_sec, status, log_output)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (f"{folder}/{script}", start, end, duration, status, out+err))
    conn.commit()

if __name__=="__main__":
    conn = mysql.connector.connect(**DB_CONFIG)
    while True:
        cur = conn.cursor()
        cur.execute("SELECT folder, script_name, run_interval, run_after, parallel, comment, duplicate_of, errors FROM cron_script_rules")
        rules = cur.fetchall()
        for rule in rules:
            folder, script, interval, run_after, parallel, *rest = rule
            if rest[-1]: continue  # є помилки — не виконуємо
            if not interval: continue
            # Перевірка часу та запуск
            if should_run(interval, None):
                if parallel=="так":
                    threading.Thread(target=process_task, args=(rule,)).start()
                else:
                    process_task(rule)
        time.sleep(60)
    conn.close()
