# file_utils/txt.py
"""
Модуль для вставки штампа "Paid ДатаЧас" у текстовий (.txt) файл.

Штамп вставляється у початок файлу, старий вміст зсувається вниз.
"""

import os

def stamp_txt(file_path: str, stamp: str):
    """
    Вставляє штамп у початок текстового файлу.

    Штамп: "Paid\nДатаЧас"
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        new_content = f"Paid\n{stamp.replace('Оплачено ', '')}\n\n{content}"

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(new_content)

    except Exception as e:
        raise Exception(f"❌ Помилка при обробці TXT: {e}")


# 🔧 Тест при запуску напряму
if __name__ == "__main__":
    test_path = r"C:\Users\la\OneDrive\Рабочий стол\Бальд Кнопка.txt"
    stamp_txt(test_path, "Оплачено 2025-07-30 19:15")
