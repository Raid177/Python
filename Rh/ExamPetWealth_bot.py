import os
import datetime
import asyncio
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties

BASE_DIR = os.path.dirname(os.path.abspath(__file__))



# Завантаження .env
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

# Ініціалізація бота
bot = Bot(
    token=os.getenv("BOT_TOKEN"),
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

# Стани FSM
class StudyStates(StatesGroup):
    waiting_study_number = State()
    waiting_images = State()

# Дозволені формати зображень
ALLOWED_EXTENSIONS = {".jpeg", ".jpg", ".png", ".dcm"}

# Команда /start
@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await message.answer("👋 Введіть номер дослідження з Єнота.")
    await state.set_state(StudyStates.waiting_study_number)

# Прийом номера дослідження
@dp.message(StudyStates.waiting_study_number)
async def receive_study_number(message: Message, state: FSMContext):
    exam_number = message.text.strip()
    if not exam_number.isalnum():
        await message.answer("⚠️ Номер дослідження має бути текстом без пробілів.")
        return

    await state.update_data(exam_number=exam_number)
    await message.answer(
        f"✅ Номер дослідження <b>{exam_number}</b> отримано.\n\n"
        "📎 Надішліть, будь ласка, знімки рентгену як <b>файли</b> (через скрепку), а не фото з галереї."
    )
    await state.set_state(StudyStates.waiting_images)

# Обробка знімків
@dp.message(StudyStates.waiting_images)
async def handle_uploaded_document(message: Message, state: FSMContext):
    if not message.document:
        await message.answer("⚠️ Надішліть, будь ласка, <b>файл</b> (через 📎). Фото з камери не підходять.")
        return

    file_name = message.document.file_name.lower()
    if not any(file_name.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        await message.answer("⚠️ Дозволені формати: .jpeg, .jpg, .png, .dcm")
        return

    data = await state.get_data()
    exam_number = data.get("exam_number", "noexam")
    save_dir = data.get("save_dir")

    if not save_dir:
        today = datetime.date.today().isoformat()
        folder_name = f"{today}_{exam_number}_unknown"
        study_root = os.path.join(BASE_DIR, "Study")
        save_dir = os.path.join(study_root, folder_name)

        os.makedirs(save_dir, exist_ok=True)
        await state.update_data(save_dir=save_dir)

    # Завантаження файлу
    file = await bot.get_file(message.document.file_id)
    file_path = os.path.join(save_dir, message.document.file_name)
    await bot.download_file(file.file_path, destination=file_path)

    await message.answer(f"✅ Збережено: <b>{message.document.file_name}</b>")

    # Після першого файлу — готуємось до виклику модуля пацієнта
    if not data.get("patient_data_loaded"):
        await message.answer("📄 Завантажено перший знімок. Отримую дані пацієнта...")
        # TODO: виклик get_patient_data(exam_number)
        await message.answer("🔧 (Заглушка) Дані пацієнта ще не реалізовано.")
        await state.update_data(patient_data_loaded=True)

# Запуск бота
if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot))
