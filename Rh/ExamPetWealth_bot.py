import os
import datetime
import asyncio
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from get_patient_data import get_patient_data, insert_study_request
from analyze_images import analyze_images
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

bot = Bot(
    token=os.getenv("BOT_TOKEN"),
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

class StudyStates(StatesGroup):
    waiting_study_number = State()
    waiting_images = State()
    waiting_feedback_decision = State()
    waiting_feedback_text = State()

ALLOWED_EXTENSIONS = {".jpeg", ".jpg", ".png", ".dcm"}

@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("👋 Введіть номер дослідження з Єнота.")
    await state.set_state(StudyStates.waiting_study_number)

@dp.message(StudyStates.waiting_study_number)
async def receive_study_number(message: Message, state: FSMContext):
    exam_number = message.text.strip()
    if not exam_number.isalnum():
        await message.answer("⚠️ Номер дослідження має бути текстом без пробілів.")
        return
    await state.update_data(exam_number=exam_number, images_saved=0)
    await message.answer(
        f"✅ Номер дослідження <b>{exam_number}</b> отримано.\n\n"
        "📎 Надішліть, будь ласка, знімки рентгену як <b>файли</b> (через скрепку), а не фото з галереї."
    )
    await state.set_state(StudyStates.waiting_images)

@dp.message(StudyStates.waiting_images)
async def handle_uploaded_document(message: Message, state: FSMContext):
    data = await state.get_data()
    exam_number = data.get("exam_number")
    if not exam_number:
        await message.answer("⚠️ Внутрішня помилка: відсутній номер дослідження. Спробуйте ще раз командою /start.")
        return

    if not message.document:
        await message.answer("⚠️ Надішліть файл формату .jpeg, .jpg, .png або .dcm через 📎.")
        return

    file_name = message.document.file_name.lower()
    if not any(file_name.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        await message.answer("⚠️ Недопустимий формат файлу. Дозволені: .jpeg, .jpg, .png, .dcm")
        return

    if not data.get("save_dir"):
        patient_info = get_patient_data(exam_number)
        if not patient_info.get("success"):
            await message.answer("❌ Помилка отримання пацієнта:\n" + patient_info["error"])
            return

        date_str = patient_info["date_exam"]
        number_str = exam_number.lstrip("0") or "0"
        name_str = patient_info["name"].strip().replace(" ", "_")
        folder_name = f"{date_str}_{number_str}_{name_str}"

        study_root = os.path.join(BASE_DIR, "Study")
        save_dir = os.path.join(study_root, folder_name)
        os.makedirs(save_dir, exist_ok=True)

        await state.update_data({
            "save_dir": save_dir,
            "ref_keyexam": patient_info["Ref_KeyEXAM"],
            "patient_info": patient_info,
            "images_saved": 0,
            "requested_by": message.from_user.full_name or message.from_user.username or str(message.from_user.id)
        })

    current_data = await state.get_data()
    save_dir = current_data["save_dir"]
    file = await bot.get_file(message.document.file_id)
    file_path = os.path.join(save_dir, message.document.file_name)
    await bot.download_file(file.file_path, destination=file_path)
    await message.answer(f"✅ Збережено: <b>{message.document.file_name}</b>")

    images_saved = current_data.get("images_saved", 0) + 1
    await state.update_data(images_saved=images_saved)

    await message.answer(f"📄 Завантажено {images_saved} знімок.")

    if images_saved >= 3 and not current_data.get("patient_message_sent"):
        await state.update_data(patient_message_sent=True)

        patient_info = current_data["patient_info"]
        ref_keyexam = current_data["ref_keyexam"]
        user_name = current_data["requested_by"]

        print(f"📍 USERNAME FOR DB: {user_name}")
        insert_study_request(patient_info, save_dir, user_name, images_saved)

        confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Так", callback_data="start_analysis")],
            [InlineKeyboardButton(text="❌ Ні", callback_data="cancel_analysis")]
        ])
        await message.answer(
            f"<b>Пацієнт:</b> {patient_info['name']}\n"
            f"🐾 Вид/Порода: {patient_info['kind']} / {patient_info['breed']}\n"
            f"⚥ Стать: {patient_info['sex']}, 🎂 Вік: {patient_info['age']}\n"
            f"⚖️ Вага: {patient_info['weight']} кг\n"
            f"📝 Показання: {patient_info['exam_context']}\n\n"
            f"❓ Почати аналіз?",
            reply_markup=confirm_kb
        )

@dp.callback_query(lambda c: c.data == "start_analysis")
async def process_start_analysis(callback_query: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    ref_keyexam = data.get("ref_keyexam")
    save_dir = data.get("save_dir")

    if not ref_keyexam:
        await callback_query.message.answer("❌ Ref_KeyEXAM не знайдено. Неможливо запустити аналіз.")
        return

    await callback_query.message.answer("🧠 Запускаю аналіз знімків, зачекайте...")
    result = analyze_images(ref_keyexam, save_dir)

    if result.get("success"):
        await callback_query.message.answer(
            f"✅ Аналіз завершено.\n\n<b>Результат:</b>\n<pre>{result.get('conclusion')}</pre>"
        )
        await state.set_state(StudyStates.waiting_feedback_decision)
        await callback_query.message.answer("💬 У вас є зауваження по заключенню?", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Так, хочу залишити відгук", callback_data="write_feedback")],
            [InlineKeyboardButton(text="🚫 Ні, можна вносити в Єнот", callback_data="send_to_enote")]
        ]))
    else:
        await callback_query.message.answer("❌ Помилка аналізу:\n" + result.get("error"))

@dp.callback_query(lambda c: c.data == "cancel_analysis")
async def process_cancel_analysis(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer("❌ Аналіз скасовано.")

@dp.callback_query(lambda c: c.data == "write_feedback")
async def feedback_request(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(StudyStates.waiting_feedback_text)
    await callback_query.message.answer("✍️ Напишіть свій відгук у відповідь на це повідомлення.")

@dp.message(StudyStates.waiting_feedback_text)
async def save_feedback_text(message: Message, state: FSMContext):
    data = await state.get_data()
    save_dir = data.get("save_dir")
    exam_number = data.get("exam_number")
    filename = os.path.join(save_dir, f"{exam_number}_feedback.txt")

    with open(filename, "w", encoding="utf-8") as f:
        f.write(message.text.strip())

    await message.answer("✅ Ваш відгук збережено.")
    await message.answer("❓ Чи вносити результат в Єнот?", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Так", callback_data="send_to_enote")],
        [InlineKeyboardButton(text="❌ Ні", callback_data="cancel_enote")]
    ]))

@dp.callback_query(lambda c: c.data == "send_to_enote")
async def send_result_to_enote(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer("📤 Внесення в Єнот запущено (модуль ще не реалізований).")

@dp.callback_query(lambda c: c.data == "cancel_enote")
async def cancel_enote_transfer(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer("❌ Внесення в Єнот скасовано.")

if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot))
