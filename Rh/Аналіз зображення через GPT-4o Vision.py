import os
import base64
import json
from pathlib import Path
from dotenv import load_dotenv
import openai

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


# === 🖼️ Шляхи до зображень ===
import glob

def get_image_paths(image_dir: str) -> list:
    return sorted(glob.glob(os.path.join(image_dir, "files_*")))


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
def parse_gpt_result(raw_text: str) -> dict:
    result = {}
    lines = raw_text.strip().splitlines()
    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
    return result

# 📋 2. Отримання мапи питань
def get_question_map() -> dict:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT question_name, question_key, subquestion_key
            FROM bot_study_question_map
        """)
        rows = cursor.fetchall()
        return {row["question_name"]: row for row in rows}

# 🧾 3. Збереження результатів у bot_study_findings
from datetime import datetime

def save_findings_to_db(ref_keyexam: str, findings: dict, full_text: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    question_map = get_question_map()

    with conn.cursor() as cursor:
        for name, answer in findings.items():
            if name not in question_map:
                print(f"⚠️ Пропущено: '{name}' не знайдено у question_map")
                continue
            q = question_map[name]
            cursor.execute("""
                INSERT INTO bot_study_findings
                (Ref_KeyEXAM, question_key, subquestion_key, open_answer, full_gpt_response, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open_answer = VALUES(open_answer),
                    full_gpt_response = VALUES(full_gpt_response),
                    updated_at = VALUES(updated_at)
            """, (
                ref_keyexam,
                q["question_key"],
                q["subquestion_key"],
                answer,
                full_text,
                now,
                now
            ))
    conn.commit()

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
    save_findings_to_db(ref_keyexam, findings, raw_text)
    mark_study_as_parsed(ref_keyexam)
    print("✅ Дані збережено у bot_study_findings та статус оновлено.")


# === 🚀 Основний запуск ===
if __name__ == "__main__":
    print("\n⏳ Аналізуємо знімки GPT-4o...\n")

    ref_key = "c316e9c8-3a65-11f0-8de8-2ae983d8a0f0"  # ← сюди вставляєш потрібний Ref_KeyEXAM

    try:
        row = get_study_request(ref_key)
        patient_info = build_patient_info(row)
        image_paths = get_image_paths(row["path_image"])
        messages = build_messages(image_paths, patient_info)

        # 🔎 Відладка: показати, що саме відправляється в GPT
        print("\n📤 Вміст запиту до GPT:")
        print(json.dumps(messages, indent=2)[:3000])  # обрізаємо до 3000 символів

        # 🧠 GPT-запит
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.3,
            max_tokens=2048
        )

        result = response.choices[0].message.content
        print("\n✅ Результат:\n")
        print(result)

        # ✅ Збереження результату
        try:
            finalize_processing(ref_key, result)
        except Exception as e:
            print(f"\n❌ Помилка при збереженні результатів: {e}")

    except Exception as e:
        print(f"\n❌ Помилка: {e}")
