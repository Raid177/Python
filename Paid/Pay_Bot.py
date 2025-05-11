import fitz
import subprocess
import tkinter as tk
from tkinter import messagebox, filedialog
from datetime import datetime
import os
import shutil
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from PIL import Image, ImageDraw, ImageFont
import requests
import pymysql
from dotenv import load_dotenv
import msvcrt
import sys
import tempfile

# === Блокування запуску другого екземпляра ===
lockfile_path = os.path.join(tempfile.gettempdir(), "stamp_paid.lock")
try:
    lock_file = open(lockfile_path, 'w')
    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
except OSError:
    print("⚠️ Програма вже запущена. Другий екземпляр не дозволено.")
    sys.exit()

# === Завантаження конфігурації з .env ===
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

log_file = os.path.join(os.path.dirname(__file__), "stamp_log.txt")

# === Підключення до БД ===
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE,
    charset='utf8mb4',
    autocommit=True
)
cursor = conn.cursor()

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

def send_telegram_reply(file_name, new_name):
    try:
        cursor.execute("""
            SELECT chat_id, message_id FROM telegram_files
            WHERE file_name = %s
            ORDER BY id DESC LIMIT 1
        """, (file_name,))
        row = cursor.fetchone()
        if row:
            chat_id, message_id = row
            requests.post(API_URL, data={
                "chat_id": chat_id,
                "text": f"✅ Оплачено: {new_name}",
                "reply_to_message_id": message_id
            })
            cursor.execute("""
                UPDATE telegram_files SET status='paid' WHERE file_name = %s
            """, (file_name,))
        else:
            log(f"⚠️ Не знайдено запис у БД для: {file_name}")
    except Exception as e:
        log(f"❌ Не вдалося надіслати повідомлення в Telegram: {e}")
