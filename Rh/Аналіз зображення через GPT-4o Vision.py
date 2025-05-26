import os
import base64
from pathlib import Path
from dotenv import load_dotenv
import openai

# === 🔐 Завантаження API ключа ===
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# === 📌 Інформація про пацієнта ===
patient_info = """
Інформація по дослідженню № 000000211
============================================================
📌 Кличка:            Лесик
📄 Номер договору:    220
👤 Власник (ПІБ):     Бондарчук Тетяна
🧬 Вид:               Кіт
🐾 Порода:            метис
⚥ Стать:             Male
🎂 Вік:               1р 1м
⚖️  Вага:             1.89 кг (від 2025-02-15)
📝 Показання:         кошеня, важке дихання, в'ялість, Т-40, 32 тис лейкоцитів (нейтрофільний лейкоцитоз)
"""

# === 📸 Шляхи до зображень ===
image_paths = [
    # r"C:\Users\la\OneDrive\Pet Wealth\Analytics\Python_script\Rh\RhTest\6b8dd78a-6f0c-486e-a70d-6000827309a3.jpg",
    r"C:\Users\la\OneDrive\Pet Wealth\Analytics\Python_script\Rh\RhTest\74ebf5a3-83d1-46a8-b65c-8a609a047711.jpg"
]

# === 🧠 Системний промпт ===
system_prompt = """
Ти ветеринарний рентгенолог. Проаналізуй рентгенівські знімки тварини.


📝 Структура висновку:

1. Характеристика знімку  
2. Якість укладки  
3. Трахея  
4. Паренхіма легень  
5. Серце та судини  
6. Плевра  
7. Середостіння  
8. Діафрагма  
9. Інші зміни в грудній клітці  
10. Випадкові знахідки поза грудною кліткою  
10.1 Невідповідність з клінічними даними (якщо є)  
11. Можливі причини виявлених змін  
12. Загальні рекомендації (в т.ч. дообстеження)  
13. Коротке рентгенологічне заключення  

"""

# === 📤 Підготовка повідомлень ===
def encode_image_to_base64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def build_message_with_images(image_paths, patient_info):
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": [{"type": "text", "text": patient_info}]}
    ]

    for path in image_paths:
        base64_image = encode_image_to_base64(path)
        messages[1]["content"].append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/jpeg;base64,{base64_image}"
            }
        })

    return messages

# === 🚀 Запуск ===
if __name__ == "__main__":
    print("⏳ Аналізуємо знімки GPT-4o...")
    try:
        messages = build_message_with_images(image_paths, patient_info)

        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.3,
            max_tokens=2000
        )

        result = response.choices[0].message.content
        print("\n✅ Результат:")
        print(result)

    except Exception as e:
        print(f"❌ Помилка: {e}")
