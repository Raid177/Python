# file_utils/pdf.py
"""
Модуль для вставки штампа "Оплачено ДатаЧас" у PDF-файл
(оновлено: PyMuPDF, зелений текст, два рядки: "Paid" + дата).
"""

import fitz  # PyMuPDF
import os

def stamp_pdf(file_path: str, stamp: str):
    """
    Додає штамп у правий верхній кут кожної сторінки PDF:
    перший рядок — "Paid", другий — дата і час.
    """
    doc = fitz.open(file_path)
    for page in doc:
        width = page.rect.width
        x = width - 180  # відступ від правого краю
        y = 60  # від верхнього краю

        # Перший рядок — "Paid"
        page.insert_text(
            (x, y),
            "Paid",
            fontsize=16,
            fontname="helv",  # стандартний шрифт (не потребує .ttf)
            color=(0, 0.6, 0),  # зелений
        )

        # Другий рядок — дата/час
        page.insert_text(
            (x, y + 20),
            stamp.replace("Оплачено ", ""),
            fontsize=12,
            fontname="helv",
            color=(0, 0.6, 0),
        )

    # Збереження в тимчасовий файл і заміна оригіналу
    temp_path = file_path + ".temp"
    doc.save(temp_path, garbage=4, deflate=True)
    doc.close()
    os.replace(temp_path, file_path)


# 🔧 Тест запуску незалежно
if __name__ == "__main__":
    test_path = r"C:\Users\la\OneDrive\Рабочий стол\На оплату!\pdft_test.pdf"
    stamp_pdf(test_path, "Оплачено 2025-07-30 19:05")
