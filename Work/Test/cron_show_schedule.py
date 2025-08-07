#!/usr/bin/env python3
import os
import re
import subprocess
from datetime import datetime
import mysql.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv("/root/Python/.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

def check_service_status(service_name="cron_scheduler_skript"):
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service_name],
            capture_output=True,
            text=True,
            check=False
        )
        status = result.stdout.strip()
        return status
    except Exception as e:
        return f"❌ error: {e}"

def get_next_run_time(interval: str, now=None):
    if not now:
        now = datetime.now()
    if not interval:
        return None

    m = re.match(r"^daily@(\d{2}):(\d{2})$", interval)
    if m:
        hh, mm = map(int, m.groups())
        next_time = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if next_time <= now:
            next_time += pd.Timedelta(days=1)
        return next_time

    m = re.match(r"^weekly@(\w{3}) (\d{2}):(\d{2})$", interval)
    if m:
        day_str, hh, mm = m.groups()
        weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        if day_str not in weekdays:
            return None
        target_weekday = weekdays.index(day_str)
        days_ahead = (target_weekday - now.weekday() + 7) % 7
        next_time = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0) + pd.Timedelta(days=days_ahead)
        if next_time <= now:
            next_time += pd.Timedelta(days=7)
        return next_time

    m = re.match(r"^monthly@(\d{1,2}) (\d{2}):(\d{2})$", interval)
    if m:
        day, hh, mm = map(int, m.groups())
        try:
            next_time = now.replace(day=day, hour=hh, minute=mm, second=0, microsecond=0)
        except ValueError:
            return None
        if next_time <= now:
            month = now.month + 1 if now.month < 12 else 1
            year = now.year if now.month < 12 else now.year + 1
            try:
                next_time = datetime(year, month, day, hh, mm)
            except ValueError:
                return None
        return next_time

    m = re.match(r"^hourly@(\d+)$", interval)
    if m:
        n = int(m.group(1))
        next_hour = (now.hour // n + 1) * n
        day_shift = next_hour // 24
        hour_final = next_hour % 24
        next_time = now.replace(hour=hour_final, minute=0, second=0, microsecond=0) + pd.Timedelta(days=day_shift)
        return next_time

    m = re.match(r"^every@(\d+)minutes$", interval)
    if m:
        n = int(m.group(1))
        minute_offset = (now.minute // n + 1) * n
        hour = now.hour + (minute_offset // 60)
        minute = minute_offset % 60
        day_shift = hour // 24
        hour = hour % 24
        next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + pd.Timedelta(days=day_shift)
        return next_time

    return None

def main():
    service_status = check_service_status()
    if service_status != "active":
        print(f"⚠️  УВАГА: Служба cron_scheduler_skript не запущена! Статус: {service_status}")
        return
    else:
        print("✅ Служба cron_scheduler_skript активна.\n")

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT folder, script_name, run_interval FROM cron_script_rules")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    now = datetime.now()
    results = []
    for folder, script, interval in rows:
        next_time = get_next_run_time(interval, now)
        if next_time is None:
            continue
        results.append({
            "script_name": script,
            "folder": folder,
            "run_interval": interval,
            "next_run_time": next_time.strftime("%Y-%m-%d %H:%M:%S"),
            "next_dt": next_time
        })

    df = pd.DataFrame(results)
    df = df.sort_values("next_dt").drop(columns="next_dt")

    print("📅 Найближчі запуски скриптів:\n")
    print(df.to_string(index=False))

if __name__ == "__main__":
    main()
