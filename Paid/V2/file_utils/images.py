# file_utils/images.py
"""
Модуль для вставки штампа "Paid ДатаЧас" на зображення (JPG, PNG).
"""

from PIL import Image, ImageDraw, ImageFont
import os

def stamp_image(file_path: str, stamp: str):
    """
    Додає зелений штамп "Paid\nДатаЧас" у правий верхній кут зображення.
    """
    try:
        image = Image.open(file_path).convert("RGBA")

        # Створюємо прозорий шар для малювання тексту
        txt_layer = Image.new("RGBA", image.size, (255, 255, 255, 0))
        draw = ImageDraw.Draw(txt_layer)

        width, height = image.size

        # Спроба завантажити системний шрифт або дефолтний
        try:
            font = ImageFont.truetype("arial.ttf", 20)
        except:
            font = ImageFont.load_default()

        text1 = "Paid"
        text2 = stamp.replace("Оплачено ", "")

        x = width - 220
        y = 20

        draw.text((x, y), text1, font=font, fill=(0, 160, 0, 255))
        draw.text((x, y + 40), text2, font=font, fill=(0, 160, 0, 255))

        # Комбінуємо зображення з прозорим шаром
        watermarked = Image.alpha_composite(image, txt_layer)

        # Зберігаємо зображення назад
        watermarked = watermarked.convert("RGB")
        watermarked.save(file_path)

    except Exception as e:
        raise Exception(f"❌ Помилка при обробці зображення: {e}")


# 🔧 Тест при запуску напряму
if __name__ == "__main__":
    test_path = r"C:\Users\la\OneDrive\Рабочий стол\paid_image_test.jpg"
    stamp_image(test_path, "Оплачено 2025-07-30 19:30")
