# handlers.py
# Завдання: обробники команд для VetAssist бота.
# Підтримує:
# - /help
# - /kontrol (команда може бути в будь-якому місці повідомлення; або як reply)
# - /pick (резервний варіант вибору)
# - числовий вибір без команди (1,2,3...) після списку

from typing import List, Dict
import sys, traceback
from telegram import Update
from telegram.ext import ContextTypes
from features.patient_control.resolve import extract_patient_numbers
from features.patient_control.enote_lookup import (
    get_patient_by_number,
    get_patient_names_by_numbers,
)

# =========================
# /help
# =========================
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник команди /help"""
    await update.message.reply_text(
        "Доступні команди:\n"
        "/start – перевірити, що бот працює\n"
        "/kontrol – контроль пацієнтів (можна писати /kontrol на початку/вкінці повідомлення або зробити reply)\n"
        "/help – ця довідка\n"
    )


# =========================
# /kontrol
# =========================
async def kontrol_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /kontrol:
    - якщо це reply → беремо текст із відповіді
    - інакше беремо весь текст цього повідомлення і прибираємо з нього маркер '/kontrol' де б він не стояв
    - якщо тексту немає → підказка
    - далі: шукаємо номери в шапках; 1 номер → одразу тягнемо GUID; кілька → список і очікування цифри
    """

    # 1) Визначаємо джерело тексту
    if update.message.reply_to_message and update.message.reply_to_message.text:
        full_text = update.message.reply_to_message.text
        print("[/kontrol] using reply_to_message text", file=sys.stderr)
    else:
        full_text = (update.message.text or "")
        print("[/kontrol] using same-message text; length=", len(full_text), file=sys.stderr)

    # Прибираємо всі входження '/kontrol' (на початку, в кінці, посередині, на окремому рядку)
    raw_text = full_text.replace("/kontrol", "").strip()

    if not raw_text:
        await update.message.reply_text(
            "Надішліть /kontrol у відповідь на повідомлення з перепискою "
            "або напишіть /kontrol і той самий текст в одному повідомленні."
        )
        return

    # 2) Лог сирого тексту (обрізаємо, щоб не засмічувати консоль)
    print("[/kontrol] --- RAW START ---", file=sys.stderr)
    print(raw_text[:2000], file=sys.stderr)
    print("[/kontrol] --- RAW END ---", file=sys.stderr)

    # 3) Витягуємо номери з шапок повідомлень
    nums: List[str] = extract_patient_numbers(raw_text)
    print(f"[/kontrol] extracted numbers: {nums}", file=sys.stderr)

    if not nums:
        await update.message.reply_text("Номери пацієнтів у дужках не знайдено в шапках повідомлень.")
        return

    # 4) Якщо номер один — тягнемо GUID/кличку/власника з Єнота
    if len(nums) == 1:
        chosen = nums[0]
        try:
            info = get_patient_by_number(chosen)
            print(f"[/kontrol] lookup {chosen} → {info}", file=sys.stderr)
        except Exception:
            print("[/kontrol] ERROR get_patient_by_number:", file=sys.stderr)
            traceback.print_exc()
            await update.message.reply_text(f"Знайшов номер {chosen}, але помилка під час звернення до Єнота.")
            return

        if not info:
            await update.message.reply_text(f"Знайшов номер {chosen}, але в Єноті такого пацієнта немає.")
            return

        context.user_data["selected_patient_number"] = chosen
        context.user_data["patient_ref_key"] = info["ref_key"]

        await update.message.reply_text(
            "✅ Обрано пацієнта:\n"
            f"- Номер: {chosen}\n"
            f"- Кличка: {info['name']}\n"
            f"- Власник: {info['owner']}\n"
            f"- Ref_Key: {info['ref_key']}\n\n"
            "Готово. Далі — передамо переписку в ChatGPT."
        )
        return

    # 5) Якщо номерів кілька — показуємо список і чекаємо цифру-вибір
    context.user_data["candidate_patient_numbers"] = nums
    context.user_data["awaiting_pick"] = True

    try:
        name_map: Dict[str, str] = get_patient_names_by_numbers(nums)  # {"1472": "Барні", ...} або ""
        print(f"[/kontrol] name_map: {name_map}", file=sys.stderr)
    except Exception:
        print("[/kontrol] ERROR get_patient_names_by_numbers:", file=sys.stderr)
        traceback.print_exc()
        name_map = {n: "" for n in nums}

    lines = []
    for i, n in enumerate(nums, start=1):
        nick = name_map.get(n) or ""
        lines.append(f"{i}) {n} — {nick}" if nick else f"{i}) {n}")

    await update.message.reply_text(
        "Знайшов кілька номерів пацієнтів:\n"
        + "\n".join(lines)
        + "\n\nНадішліть просто цифру (1, 2, 3...), щоб обрати пацієнта."
    )


# =========================
# /pick (резерв)
# =========================
async def pick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /pick <значення>
    Приймає або сам номер пацієнта (наприклад: 1472), або порядковий номер у списку (наприклад: 1).
    Зберігає вибір у user_data["selected_patient_number"].
    """
    parts = (update.message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text("Вкажіть: /pick <номер_пацієнта> або /pick <порядковий_номер>")
        return

    value = parts[1].strip()
    candidates = context.user_data.get("candidate_patient_numbers", [])

    chosen = None
    if value.isdigit():
        idx = int(value) - 1
        if 0 <= idx < len(candidates):
            chosen = candidates[idx]
        elif value in candidates:
            chosen = value
    else:
        if value in candidates:
            chosen = value

    if not chosen:
        pretty = ", ".join(candidates) if candidates else "нічого"
        await update.message.reply_text(
            f"Не вдалося інтерпретувати '{value}'. Оберіть одне з: {pretty}"
        )
        return

    context.user_data["selected_patient_number"] = chosen
    await update.message.reply_text(f"✅ Обрано номер пацієнта: {chosen}\n(Далі — пошук GUID у Єноті)")


# =========================
# Числовий вибір без команди
# =========================
async def number_pick_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Якщо користувачеві щойно показали список і він надсилає просто число (1,2,3...),
    трактуємо це як вибір із candidate_patient_numbers і одразу тягнемо GUID/кличку.
    """
    text = (update.message.text or "").strip()
    if not text.isdigit():
        return

    if not context.user_data.get("awaiting_pick"):
        return  # не в режимі вибору — ігноруємо

    candidates = context.user_data.get("candidate_patient_numbers", [])
    idx = int(text) - 1
    if 0 <= idx < len(candidates):
        chosen = candidates[idx]
        context.user_data["selected_patient_number"] = chosen
        context.user_data["awaiting_pick"] = False

        # одразу тягнемо GUID/кличку/власника
        try:
            info = get_patient_by_number(chosen)
        except Exception:
            traceback.print_exc()
            await update.message.reply_text(f"Обрано {chosen}, але сталася помилка під час звернення до Єнота.")
            return

        if not info:
            await update.message.reply_text(f"Обрано {chosen}, але в Єноті такого пацієнта не знайдено.")
            return

        context.user_data["patient_ref_key"] = info["ref_key"]
        await update.message.reply_text(
            "✅ Обрано пацієнта:\n"
            f"- Номер: {chosen}\n"
            f"- Кличка: {info['name']}\n"
            f"- Власник: {info['owner']}\n"
            f"- Ref_Key: {info['ref_key']}\n\n"
            "Готово. Далі — передамо переписку в ChatGPT."
        )
    else:
        await update.message.reply_text("Такого пункту немає. Надішліть номер із показаного списку.")
