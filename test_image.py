import requests
import base64
import os
from datetime import datetime
import random
import hashlib
import re

# === Конфігурація ===
USERNAME = "odata"
PASSWORD = "zX8a7M36yU"
APIKEY = "917881f0-62a2-4f37-a826-bf08ef581239"
BASE_URL = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262-copy"
HEADERS = {"apikey": APIKEY}
AUTH = (USERNAME, PASSWORD)

# === Очистка description для файлової системи ===
def clean_description(text):
    text = (text or "").strip()
    text = text.replace("\n", "_").replace("\r", "").replace(" ", "_")
    text = re.sub(r'[\\/*?:"<>|]', "_", text)
    if not text or text == ".":
        return "file"
    return text

# === Завантажити файли по documentId ===
def get_attached_files(document_id: str, document_type="diagnostic"):
    url = f"{BASE_URL}/hs/api/v2/AttachedFiles"
    params = {"documentId": document_id, "documentType": document_type}
    resp = requests.get(url, headers=HEADERS, auth=AUTH, params=params)

    if resp.status_code != 200:
        print(f"[!] Помилка запиту: {resp.status_code}")
        print(resp.text)
        return []

    files = resp.json()
    print(f"[i] Отримано {len(files)} файлів з документа {document_id}")
    return files

# === Зберегти файли локально ===
def save_files(files, target_folder="downloaded_from_enote"):
    os.makedirs(target_folder, exist_ok=True)

    for idx, file in enumerate(files):
        ext = file.get("fileExtension", ".bin").replace(".", "")
        file_data = file.get("fileData")
        if not file_data:
            print(f"[!] Пропущено файл {idx} — немає fileData")
            continue

        name_raw = clean_description(file.get("description", ""))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        suffix = f"{timestamp}_{random.randint(100, 999)}"
        name = f"{name_raw}_{suffix}"

        path = os.path.join(target_folder, f"{name}.{ext}")
        binary = base64.b64decode(file_data)
        with open(path, "wb") as f:
            f.write(binary)

        hash_ = hashlib.md5(binary).hexdigest()
        print(f"[+] Файл збережено: {path} | hash: {hash_}")

# === Точка входу ===
if __name__ == "__main__":
    DOCUMENT_ID = "c316e9c8-3a65-11f0-8de8-2ae983d8a0f0"
    files = get_attached_files(DOCUMENT_ID)
    save_files(files)
