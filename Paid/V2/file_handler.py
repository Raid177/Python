# file_handler.py
"""
Модуль для обробки файлів після підтвердження оплати:
1. Додає штамп "Оплачено ДатаЧас" у файл:
   - PDF: малюнок у правий верхній кут
   - TXT: текстовий рядок на початку
   - DOC(X), XLS(X): вставка тексту (мінімально)
   - JPG/PNG: водяний знак
2. Переміщує файл у підпапку "Оплачено" в тій самій директорії
3. Логує всі дії в консоль
"""

import os
import shutil
from datetime import datetime
from log import log

from file_utils.pdf import stamp_pdf
from file_utils.txt import stamp_txt
from file_utils.images import stamp_image
from file_utils.word import stamp_doc
from file_utils.excel import stamp_excel

STAMP_TEXT = "Оплачено"

def process_paid_file(file_path: str):
    ext = os.path.splitext(file_path)[1].lower()
    file_name = os.path.basename(file_path)
    folder = os.path.dirname(file_path)
    paid_dir = os.path.join(folder, "Оплачено")
    os.makedirs(paid_dir, exist_ok=True)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    stamp = f"{STAMP_TEXT} {now_str}"

    log(f"🔧 Обробка файлу: {file_name} ({ext})")
    log(f"📌 Штамп: {stamp}")

    try:
        if ext == ".pdf":
            log("➡️ Викликається stamp_pdf()")
            stamp_pdf(file_path, stamp)
            log("✅ stamp_pdf() завершено")
        elif ext == ".txt":
            log("➡️ Викликається stamp_txt()")
            stamp_txt(file_path, stamp)
            log("✅ stamp_txt() завершено")
        elif ext in (".jpg", ".jpeg", ".png"):
            log("➡️ Викликається stamp_image()")
            stamp_image(file_path, stamp)
            log("✅ stamp_image() завершено")
        elif ext in (".doc", ".docx"):
            log("➡️ Викликається stamp_doc()")
            stamp_doc(file_path, stamp)
            log("✅ stamp_doc() завершено")
        elif ext in (".xls", ".xlsx"):
            log("➡️ Викликається stamp_excel()")
            stamp_excel(file_path, stamp)
            log("✅ stamp_excel() завершено")
        else:
            log(f"⚠️ Невідомий тип файлу: {ext}")
            return

        # Переміщення
        new_path = os.path.join(paid_dir, file_name)
        log(f"📦 Спроба перемістити файл до: {new_path}")
        shutil.move(file_path, new_path)
        log(f"📁 Переміщено до: {new_path}")
        log(f"✅ Опрацьовано та переміщено: {file_name}")

    except Exception as e:
        log(f"❌ Виняток у process_paid_file для {file_name}: {e}")
        raise
