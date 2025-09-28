# bot/routers/client.py
from aiogram import Router, F, Bot
from aiogram.types import Message, Contact
from aiogram.filters import Command

from core.db import get_conn
from core.config import settings
from core.repositories import clients as repo_c

from bot.keyboards.common import ask_phone_kb, main_menu_kb, privacy_inline_kb

router = Router()

WELCOME = (
    "Вітаємо у клініці PetWealth! 💚\n"
    f"Ми раді допомогти вам. Якщо питання термінове — телефонуйте {settings.SUPPORT_PHONE}.\n"
    "Зверніть увагу: чат не відслідковується постійно, але ми відповімо, щойно побачимо повідомлення."
)

PHONE_EXPLAIN = (
    "Щоб ідентифікувати вас як власника тварини і надалі надсилати нагадування "
    "про візити/вакцинації, результати аналізів тощо — поділіться номером телефону.\n\n"
    "Це добровільно. Ви можете натиснути «Пропустити»."
)

async def _is_staff_member(bot: Bot, user_id: int) -> bool:
    try:
        cm = await bot.get_chat_member(settings.support_group_id, user_id)
        return cm.status in ("creator", "administrator", "member")
    except Exception:
        return False

@router.message(Command("start"), F.chat.type == "private")
async def client_start(message: Message, bot: Bot):
    # якщо користувач — співробітник, підкажемо використовувати агентський /start у приваті
    if await _is_staff_member(bot, message.from_user.id):
        await message.answer(
            "Вітаю! Ви у команді PetWealth 🐾\n"
            "Для роботи з клієнтами використовуйте ваші агентські команди: /setname, та відповіді з тем у службовій групі.\n"
            "Якщо потрібно протестувати клієнтський сценарій — напишіть із тестового не-робочого акаунта."
        )
        return

    conn = get_conn()
    try:
        c = repo_c.get_client(conn, message.from_user.id)
        if not c or not c.get("phone"):
            repo_c.upsert_client(conn, message.from_user.id, None, False)
    finally:
        conn.close()

    await message.answer(PHONE_EXPLAIN, reply_markup=ask_phone_kb())
    await message.answer(
        "Натисніть кнопку нижче, щоб переглянути політику конфіденційності.",
        reply_markup=privacy_inline_kb(settings.PRIVACY_URL)
    )

@router.message(F.contact, F.chat.type == "private")
async def got_contact(message: Message):
    contact: Contact = message.contact
    if not contact or not contact.phone_number:
        await message.answer(
            "Не вдалося отримати номер. Ви можете спробувати ще раз або натиснути «Пропустити».",
            reply_markup=ask_phone_kb()
        )
        return

    conn = get_conn()
    try:
        repo_c.upsert_client(conn, message.from_user.id, contact.phone_number, True)
    finally:
        conn.close()

    await message.answer("Дякуємо! Номер збережено ✅", reply_markup=main_menu_kb())
    await message.answer(WELCOME)

@router.message(F.text == "➡️ Пропустити", F.chat.type == "private")
async def skip_phone(message: Message):
    await message.answer("Добре, пропускаємо. Ви завжди зможете надіслати номер пізніше.", reply_markup=main_menu_kb())
    await message.answer(WELCOME)

# Кнопки швидкого старту (на MVP просто дають інструкцію)
@router.message(F.text == "🩺 Запитання по поточному лікуванню", F.chat.type == "private")
async def btn_current_treatment(message: Message):
    await message.answer("Опишіть, будь ласка, ваше питання щодо поточного лікування.")

@router.message(F.text == "📅 Записатись на прийом", F.chat.type == "private")
async def btn_booking(message: Message):
    await message.answer("Напишіть зручний день/час та ім’я пацієнта.")

@router.message(F.text == "❓ Задати питання", F.chat.type == "private")
async def btn_question(message: Message):
    await message.answer("Напишіть ваше питання.")

@router.message(F.text == "🗺 Як нас знайти", F.chat.type == "private")
async def btn_nav(message: Message):
    await message.answer(
        "📍 Київ, вул. ...\n🕒 Пн-Нд 08:00–22:00\n"
        f"☎️ {settings.SUPPORT_PHONE}\n"
        "Google Maps: https://maps.app.goo.gl/Rir8Qgmzotz3RZMU7"
    )
