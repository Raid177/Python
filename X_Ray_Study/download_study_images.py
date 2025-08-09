"""
МОДУЛЬ: download_study_images.py

ОПИС:
Цей модуль завантажує знімки досліджень із Єнота (API AttachedFiles) у локальну файлову систему.
Використовується для автоматичного збору рентгенівських зображень після створення запиту.

Режими роботи:

🟩 1. Функція: download_images(ref_key_exam: str, folder_path: str) -> int
    - Завантажує знімки для конкретного дослідження
    - Зберігає файли у вказану папку як JPEG
    - Назви файлів формуються з опису знімку (description), або "No_Description"
    - Повертає кількість збережених файлів

🟨 2. Автономний запуск (__main__)
    - Шукає всі записи у таблиці xr_study_requests зі статусом "waiting" і image_count = 0
    - По черзі викликає download_images() для кожного запису
    - Формує ім’я папки: Study/YYYY-MM-DD_STUDYNUM_NAME
    - Після успішного завантаження оновлює:
        - status = 'done'
        - image_count
        - image_folder

Використовує авторизацію через BASE_URL (API AttachedFiles) і логін/пароль із .env
"""

import os
import re
import base64
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

# === Load .env ===
load_dotenv()
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# === API AttachedFiles ===
API_KEY = "917881f0-62a2-4f37-a826-bf08ef581239"
BASE_URL = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262-copy"
HEADERS = {"apikey": API_KEY}
AUTH = (ODATA_USER, ODATA_PASSWORD)

# === Функція очищення назв ===
def clean_text(text):
    text = (text or "").strip()
    text = text.replace("\n", "_").replace("\r", "").replace(" ", "_")
    text = re.sub(r'[\\/*?:"<>|]', "_", text)
    if not text or text == ".":
        return "No_Description"
    return text

# === Підключення до БД ===
def get_db():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE,
    )

# === Отримати прикріплені файли по ref_key_exam ===
def get_attached_files(document_id: str):
    url = f"{BASE_URL}/hs/api/v2/AttachedFiles"
    params = {"documentId": document_id, "documentType": "diagnostic"}
    resp = requests.get(url, headers=HEADERS, auth=AUTH, params=params)

    if resp.status_code != 200:
        print(f"[!] Запит помилки {resp.status_code}: {resp.text}")
        return []

    return resp.json()

# === Зберегти файли у вказану папку ===
def download_images(ref_key_exam: str, folder_path: str) -> int:
    files = get_attached_files(ref_key_exam)
    if not files:
        print(f"[!] Не знайдено зображень для {ref_key_exam}")
        return 0

    Path(folder_path).mkdir(parents=True, exist_ok=True)
    count = 0

    for idx, file in enumerate(files, 1):
        file_data = file.get("fileData")
        if not file_data:
            continue

        description = clean_text(file.get("description"))
        filename = f"{description}_{idx}.jpg"
        full_path = os.path.join(folder_path, filename)

        try:
            with open(full_path, "wb") as f:
                f.write(base64.b64decode(file_data))
            count += 1
        except Exception as e:
            print(f"[!] Помилка збереження {filename}: {e}")

    print(f"[✓] Збережено {count} зображень у {folder_path}")
    return count

# === Оновити запис у xr_study_requests ===
def update_request(ref_key_exam, image_count, folder_path):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE xr_study_requests
        SET status = 'downloaded',
            image_count = %s,
            image_folder = %s,
            updated_at = NOW()
        WHERE ref_key_exam = %s
    """, (image_count, folder_path, ref_key_exam))
    conn.commit()
    cursor.close()
    conn.close()

# === Обробити всі очікуючі дослідження (режим __main__) ===
def process_all_pending():
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ref_key_exam, exam_date, study_number, patient_name
        FROM xr_study_requests
        WHERE status = 'waiting' AND image_count = 0
        ORDER BY created_at ASC
    """)
    studies = cursor.fetchall()
    cursor.close()
    conn.close()

    if not studies:
        print("[✓] Немає досліджень для завантаження.")
        return

    for study in studies:
        exam_date = study["exam_date"].strftime("%Y-%m-%d")
        folder_name = f"{exam_date}_{study['study_number']}_{study['patient_name']}"
        folder_name = clean_text(folder_name)
        folder_path = os.path.join("Study", folder_name)

        print(f"[→] Завантаження: {study['study_number']} / {study['patient_name']}")
        count = download_images(study["ref_key_exam"], folder_path)
        update_request(study["ref_key_exam"], count, folder_path)

# === MAIN ===
if __name__ == "__main__":
    process_all_pending()
