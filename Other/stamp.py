import fitz
import subprocess
import tkinter as tk
from tkinter import messagebox
from datetime import datetime
import os
import shutil

pdf_path = r"C:\Users\la\OneDrive\Рабочий стол\На оплату!\test.pdf"
editor_path = r"C:\Program Files\Tracker Software\PDF Editor\PDFXEdit.exe"
target_folder = r"C:\Users\la\OneDrive\Рабочий стол\На оплату!\Оплачено"

print("🔄 Відкриваємо PDF у редакторі...")
viewer = subprocess.Popen([editor_path, pdf_path])
viewer.wait()
print("✅ Редактор закрито.")

root = tk.Tk()
root.withdraw()
root.attributes("-topmost", True)
answer = messagebox.askyesno("Підтвердження", "Внести штамп 'PAID'?")

if answer:
    print("✍️ Вносимо штамп...")
    doc = fitz.open(pdf_path)
    page = doc[0]

    stamp_text = "PAID"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    x = page.rect.width - 200
    y = 50
    fontsize = 18
    color = (0, 0.6, 0)

    page.insert_text((x, y), stamp_text, fontsize=fontsize, color=color)
    page.insert_text((x, y + 25), timestamp, fontsize=fontsize, color=color)

    # Створюємо папку, якщо потрібно
    os.makedirs(target_folder, exist_ok=True)

    # Формуємо нове ім’я з датою
    filename = os.path.basename(pdf_path)
    ts = datetime.now().strftime("%Y-%m-%d %H-%M")
    new_name = f"{ts} {filename}"
    new_path = os.path.join(target_folder, new_name)

    # Зберігаємо одразу у фінальне місце
    doc.save(new_path, garbage=4, deflate=True)
    doc.close()

    # Видаляємо оригінал
    os.remove(pdf_path)

    print(f"✅ Штамп додано і файл переміщено до:\n{new_path}")
else:
    print("❌ Внесення штампа скасовано.")
