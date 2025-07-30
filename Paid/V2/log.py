# log.py
from datetime import datetime
import os
import sys

def get_base_dir():
    """Повертає шлях до папки, де лежить .py або .exe."""
    if getattr(sys, 'frozen', False):  # Якщо це .exe
        return os.path.dirname(sys.executable)
    else:  # Якщо це звичайний .py
        return os.path.dirname(__file__)

LOG_FILE = os.path.join(get_base_dir(), "stamp_log.txt")

def log(message: str):
    """Записує повідомлення у лог-файл з часовою міткою."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
