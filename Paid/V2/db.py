"""
db.py
📌 Модуль для підключення до MySQL (MariaDB) через параметри з .env-файлу.
🔹 Підтримує підключення до серверної БД Hetzner (через змінні з префіксом _Serv).
🔹 Автоматично підключаєсь до тунелю (127.0.0.1:3307), якщо вказано в .env.
"""

import pymysql
import os
from dotenv import load_dotenv
from log import log

# Завантаження .env
load_dotenv()

# Отримання параметрів з .env (для Hetzner)
DB_HOST = os.getenv("DB_HOST_Serv", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT_Serv", 3306))
DB_USER = os.getenv("DB_USER_Serv")
DB_PASSWORD = os.getenv("DB_PASSWORD_Serv")
DB_DATABASE = os.getenv("DB_DATABASE_Serv")

def get_connection():
    """Створює з'єднання з базою даних і повертає його."""
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            charset="utf8mb4",
            autocommit=True
        )
        log(f"✅ Підключено до БД: {DB_HOST}:{DB_PORT}/{DB_DATABASE}")
        return conn
    except Exception as e:
        log(f"❌ Помилка підключення до БД: {e}")
        raise

def get_server_connection():
    """Аліас до get_connection() для сумісності з іншими модулями."""
    return get_connection()