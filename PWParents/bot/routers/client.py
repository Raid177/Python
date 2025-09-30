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

# =============================
# КЛІЄНТ → ТЕМА САППОРТ-ГРУПИ
# =============================
from core.repositories import tickets as repo_t
from core.services.relay import log_and_send_text_to_topic, log_inbound_media_copy
# from aiogram.filters import CommandStart

async def _ensure_ticket_for_client(bot: Bot, client_id: int) -> dict:
    """
    Повертає відкритий тікет клієнта. Якщо відкритого немає —
    ПЕРЕВІДКРИВАЄ останній тікет і пише в його ж тему.
    Якщо теми нема/зламана — створює нову і оновлює thread_id в останньому тікеті.
    """
    # 1) якщо вже є відкритий або "in_progress" — просто повертаємо його
    conn = get_conn()
    try:
        t = repo_t.find_open_by_client(conn, client_id)
    finally:
        conn.close()
    if t:
        return t

    # 2) відкритих немає → беремо ОСТАННІЙ тікет цього клієнта
    conn = get_conn()
    try:
        last = repo_t.find_latest_by_client(conn, client_id)
    finally:
        conn.close()

    if last:
        thread_id = last.get("thread_id")
        # якщо теми немає — створимо нову і прив'яжемо ДО ТОГО Ж тікета
        if not thread_id:
            topic = await bot.create_forum_topic(chat_id=settings.support_group_id, name=f"ID{client_id}")
            conn = get_conn()
            try:
                repo_t.update_thread(conn, last["id"], topic.message_thread_id)
                repo_t.reopen(conn, last["id"])
                last = repo_t.get_by_id(conn, last["id"])
            finally:
                conn.close()
            # службова нотатка + картка
            await bot.send_message(
                chat_id=settings.support_group_id,
                message_thread_id=last["thread_id"],
                text=f"🟢 Перевідкрито звернення клієнта <code>{last['label'] or client_id}</code>."
            )
            await bot.send_message(
                chat_id=settings.support_group_id,
                message_thread_id=last["thread_id"],
                text=(f"🟢 Заявка\nКлієнт: <code>{last['label'] or last['client_user_id']}</code>\n"
                      f"Статус: {last['status']}")
            )
            return last

        # тема існує → просто перевідкриваємо останній тікет і пишемо в нього
        conn = get_conn()
        try:
            repo_t.reopen(conn, last["id"])
            last = repo_t.get_by_id(conn, last["id"])
        finally:
            conn.close()

        await bot.send_message(
            chat_id=settings.support_group_id,
            message_thread_id=last["thread_id"],
            text=f"🟢 Перевідкрито звернення клієнта <code>{last['label'] or client_id}</code>."
        )
        await bot.send_message(
            chat_id=settings.support_group_id,
            message_thread_id=last["thread_id"],
            text=(f"🟢 Заявка\nКлієнт: <code>{last['label'] or last['client_user_id']}</code>\n"
                  f"Статус: {last['status']}")
        )
        return last

    # 3) узагалі немає історії → створюємо ПЕРШИЙ тікет і тему
    topic = await bot.create_forum_topic(chat_id=settings.support_group_id, name=f"ID{client_id}")
    conn = get_conn()
    try:
        ticket_id = repo_t.create(conn, client_id, topic.message_thread_id)
        t = repo_t.get_by_id(conn, ticket_id)
    finally:
        conn.close()

    await bot.send_message(
        chat_id=settings.support_group_id,
        message_thread_id=t["thread_id"],
        text=(f"🟢 Заявка\nКлієнт: <code>{t['label'] or t['client_user_id']}</code>\n"
              f"Статус: {t['status']}")
    )
    return t

@router.message(F.chat.type == "private")
async def inbound_from_client(message: Message, bot: Bot):
    # відсікаємо службові/команди
    if message.text and message.text.startswith("/"):
        return
    # якщо це співробітник — ігноруємо (щоб не засмічувати теми при їх тестах)
    if await _is_staff_member(bot, message.from_user.id):
        return

    # знайти/створити тікет і штовхнути у тему
    t = await _ensure_ticket_for_client(bot, message.from_user.id)
    label = t.get("label") or f"{message.from_user.id}"
    head = f"📨 Від клієнта <code>{label}</code>"

    if message.content_type == "text":
        await log_and_send_text_to_topic(bot, settings.support_group_id, t["thread_id"], t["id"], message.text, head)
    else:
        await log_inbound_media_copy(message, settings.support_group_id, t["thread_id"], t["id"], head, bot)
