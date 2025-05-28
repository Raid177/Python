import os
import base64
import json
from pathlib import Path
from dotenv import load_dotenv
import openai
import glob
import pydicom
from PIL import Image
import numpy as np
import mimetypes
from collections import OrderedDict


# === 🌱 Завантаження ключів ===
load_dotenv(dotenv_path=r"C:\Users\la\OneDrive\Pet Wealth\Analytics\Python_script\.env")

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("❌ OPENAI_API_KEY не знайдено в .env")
openai.api_key = api_key

# === 🧠 Завантаження системного промпта з файлу ===
system_prompt_path = Path(__file__).parent / "thorax.prmt"
if not system_prompt_path.exists():
    raise FileNotFoundError("❌ Файл thorax.prmt не знайдено в директорії скрипта")
system_prompt = system_prompt_path.read_text(encoding="utf-8")

# === 🐾 Інфо пацієнта ===
def build_patient_info(row: dict) -> str:
    return f"""
📌 Кличка:            {row['name']}
📄 Номер договору:    {row['id_patient']}
👤 Власник (ПІБ):     {row['owner']}
🧬 Вид:               {row['kind']}
🐾 Порода:            {row['breed']}
⚥ Стать:             {row['sex']}
🎂 Вік:               {row['age']}
⚖️  Вага:              {row['weight']} кг
📝 Показання:          {row['exam_context']}
""".strip()

# === 🖼️ Шляхи до зображень (перевірка MIME) ===
def get_image_paths(image_dir: str) -> list:
    all_files = glob.glob(os.path.join(image_dir, "files_*"))
    image_files = []

    for path in all_files:
        ext = os.path.splitext(path)[1].lower()
        mime_type, _ = mimetypes.guess_type(path)

        # Стандартні зображення
        if mime_type and mime_type.startswith("image/"):
            image_files.append(path)

        # DICOM → перетворити
        elif ext == ".dcm":
            jpg_path = path + ".jpg"
            if not os.path.exists(jpg_path):
                result = convert_dcm_to_jpg(path, jpg_path)
                if result:
                    image_files.append(result)
            else:
                image_files.append(jpg_path)

    return sorted(image_files)

# функція для конвертації .dcm у .jpg:
def convert_dcm_to_jpg(dcm_path: str, output_path: str) -> str:
    try:
        ds = pydicom.dcmread(dcm_path)
        pixel_array = ds.pixel_array
        if len(pixel_array.shape) == 3:  # Якщо 3D або RGB
            image = Image.fromarray(pixel_array)
        else:  # 2D grayscale
            image = Image.fromarray(pixel_array).convert("L")
        image.save(output_path, "JPEG")
        return output_path
    except Exception as e:
        print(f"❌ Не вдалося конвертувати {dcm_path} → JPG: {e}")
        return None


# === 🔧 Функція для конвертації в base64 ===
def encode_image_to_base64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

# === 📤 Підготовка повідомлення ===
def build_messages(image_paths, patient_info):
    user_block = [
        {"type": "text", "text": patient_info},
        {"type": "text", "text": "Проаналізуй знімки та сформуй рентгенологічний висновок за вказаною структурою."}
    ]
    for path in image_paths:
        filename = os.path.basename(path)
        base64_image = encode_image_to_base64(path)
        print(f"✅ Додано зображення: {filename}")
        user_block.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/jpeg;base64,{base64_image}"
            }
        })

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_block}
    ]
    return messages

#   Блок 1: Отримання даних з bot_study_requests
import pymysql
from dotenv import dotenv_values

# ==== 1. Підключення до БД ====
env = dotenv_values("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

conn = pymysql.connect(
    host=env["DB_HOST"],
    user=env["DB_USER"],
    password=env["DB_PASSWORD"],
    database=env["DB_DATABASE"],
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# ==== 2. Зчитуємо пацієнта по Ref_KeyEXAM ====
def get_study_request(ref_keyexam: str) -> dict:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM bot_study_requests
            WHERE Ref_KeyEXAM = %s AND status = 'pending'
        """, (ref_keyexam,))
        result = cursor.fetchone()
        if not result:
            raise ValueError(f"❌ Не знайдено запис з Ref_KeyEXAM = {ref_keyexam} або він не має статусу 'pending'")
        return result
    
# 🧩 1. Парсер відповіді GPT (спрощений)
def parse_gpt_result(text: str) -> OrderedDict:
    findings = OrderedDict()
    
    # Приклад парсингу (залиши свою логіку тут):
    sections = text.split("\n\n")
    for section in sections:
        if section.strip():
            lines = section.strip().split(":", 1)
            if len(lines) == 2:
                title = lines[0].strip()
                body = lines[1].strip()
                findings[title] = body

    # Додай лог для перевірки порядку
    for i, (title, answer) in enumerate(findings.items(), start=1):
        print(f"{i}. {title[:60]}...")

    return findings

# 📋 2. Отримання мапи питань
def get_question_map() -> dict:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT gpt_answer_number, Вопрос_Key, ЭлементарныйВопрос_Key, Description_Енота
                FROM bot_study_question_map
                WHERE active = 1
        """)
        rows = cursor.fetchall()
        return {row["Description_Енота"]: row for row in rows}

# 🧾 3. Збереження результатів у bot_study_findings
from datetime import datetime

# 🔄 4. Оновлення статусу parsed
def mark_study_as_parsed(ref_keyexam: str):
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE bot_study_requests
            SET status = 'parsed', updated_at = NOW()
            WHERE Ref_KeyEXAM = %s
        """, (ref_keyexam,))
    conn.commit()

# 🚀 5. Повний виклик:
def finalize_processing(ref_keyexam: str, raw_text: str):
    findings = parse_gpt_result(raw_text)
    question_map = get_question_map()
    
    successfully_saved = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with conn.cursor() as cursor:
        for idx, (title, answer) in enumerate(findings.items(), start=1):
            qmap = next((q for q in question_map.values() if q["gpt_answer_number"] == idx), None)

            if not qmap:
                print(f"⚠️ Пропущено: '{title}' (GPT #{idx}) — не знайдено в question_map")
                continue

            print(f"✅ Порівняння #{idx}: '{title}' → знайдено → заповнення")

            cursor.execute("""
                INSERT INTO bot_study_findings
                (Ref_KeyEXAM, question_key, subquestion_key, open_answer, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open_answer = VALUES(open_answer),
                    updated_at = VALUES(updated_at)
            """, (
                ref_keyexam,
                qmap["Вопрос_Key"],
                qmap["ЭлементарныйВопрос_Key"],
                answer,
                now,
                now
            ))

            successfully_saved += 1
    conn.commit()

    if successfully_saved:
        mark_study_as_parsed(ref_keyexam)
        print(f"\n✅ Збережено {successfully_saved} відповідей. Статус оновлено.")
    else:
        print("\n⚠️ Жодна відповідь не збережена — статус не змінено.")

# Збереження питання-відповіді в файл
def save_answer_log(image_paths, messages, gpt_response):
    try:
        folder_path = os.path.dirname(image_paths[0]) if image_paths else "."

        with open(os.path.join(folder_path, "answer.txt"), "w", encoding="utf-8") as f:
            f.write("=== 📌 Пацієнт ===\n")
            for block in messages[1]['content']:
                if block["type"] == "text":
                    f.write(f"{block['text']}\n\n")

            f.write("=== 📤 Повний запит до GPT ===\n")
            for block in messages[1]['content']:
                if block["type"] == "text":
                    f.write(f"[Text]\n{block['text']}\n\n")
                elif block["type"] == "image_url":
                    f.write("[Image] data:image/jpeg;base64,... (обрізано)\n\n")

            f.write("=== 📥 Відповідь GPT ===\n")
            f.write(gpt_response.strip() + "\n")

            f.write("\n=== 📎 Список зображень ===\n")
            for path in image_paths:
                f.write(f"{os.path.basename(path)}\n")

        print("✅ answer.txt збережено.")
    except Exception as e:
        print(f"⚠️ Не вдалося зберегти answer.txt: {e}")



# === 🚀 Основний запуск ===
if __name__ == "__main__":
    print("\n⏳ Аналізуємо знімки GPT-4o...\n")

    ref_key = "5f2a3af6-3bdb-11f0-82a9-2ae983d8a0f0"  # ← сюди вставляєш потрібний Ref_KeyEXAM

    try:
        row = get_study_request(ref_key)
        patient_info = build_patient_info(row)
        image_paths = get_image_paths(row["path_image"])
        messages = build_messages(image_paths, patient_info)

        # 🔎 Відладка: показати, що саме відправляється в GPT
        print("\n📤 Вміст запиту до GPT:")
        # print(json.dumps(messages, indent=2)[:3000])  # обрізаємо до 3000 символів

        # 🧠 GPT-запит
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.3,
            max_tokens=2048
        )

        result = response.choices[0].message.content
        save_answer_log(image_paths, messages, result)

        print("\n✅ Результат:\n")
        print(result)

        # ✅ Збереження результату
        try:
            finalize_processing(ref_key, result)
        except Exception as e:
            print(f"\n❌ Помилка при збереженні результатів: {e}")

    except Exception as e:
        print(f"\n❌ Помилка: {e}")


def analyze_images(ref_keyexam: str, image_dir: str) -> dict:
    try:
        row = get_study_request(ref_keyexam)
        patient_info = build_patient_info(row)
        image_paths = get_image_paths(image_dir)
        messages = build_messages(image_paths, patient_info)

        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.3,
            max_tokens=2048
        )
        result = response.choices[0].message.content

        save_answer_log(image_paths, messages, result)
        finalize_processing(ref_keyexam, result)

        return {"success": True, "conclusion": result.strip()}

    except Exception as e:
        return {"success": False, "error": str(e)}
