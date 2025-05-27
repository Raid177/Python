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
patient_info = """
📌 Кличка:            Оріон
📄 Номер договору:    858
👤 Власник (ПІБ):     Шеремет Дарина Сергіївна
🧬 Вид:               Кіт
🐾 Порода:            метис
⚥ Стать:             Male
🎂 Вік:               3р 0м
⚖️  Вага:              4.5 кг (від 2025-05-26)
📝 Показання:          Кашель останні 2,5 років, поступово наростає в динаміці. Контролюється, але не ефективно апоквелем. 
"""

# === 🖼️ Шляхи до зображень ===
image_paths = [
    r"C:\Users\la\OneDrive\Pet Wealth\Analytics\Python_script\Rh\RhTest\RL1.jpg",
    r"C:\Users\la\OneDrive\Pet Wealth\Analytics\Python_script\Rh\RhTest\RL2.jpg",
    r"C:\Users\la\OneDrive\Pet Wealth\Analytics\Python_script\Rh\RhTest\VD.jpg"
]

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

# === 🚀 Основний запуск ===
if __name__ == "__main__":
    print("\n⏳ Аналізуємо знімки GPT-4o...\n")

    try:
        messages = build_messages(image_paths, patient_info)

        # Вивід всього запиту у json-форматі
        # print(json.dumps(messages, indent=2)[:3000])

        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.3,
            max_tokens=2048
        )

        result = response.choices[0].message.content
        print("\n✅ Результат:\n")
        print(result)

    except Exception as e:
        print(f"\n❌ Помилка: {e}")
