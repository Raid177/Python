# bot/routers/client.py
from aiogram import Router, F, Bot
from aiogram.types import Message, Contact
from aiogram.filters import Command

from core.db import get_conn
from core.config import settings
from core.repositories import clients as repo_c
from core.repositories import tickets as repo_t
from core.services.relay import log_and_send_text_to_topic, log_inbound_media_copy

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

# -------------------- helpers --------------------

async def _is_staff_member(bot: Bot, user_id: int) -> bool:
    try:
        cm = await bot.get_chat_member(settings.support_group_id, user_id)
        return cm.status in ("creator", "administrator", "member")
    except Exception:
        return False


async def _client_display_name(bot: Bot, user_id: int) -> str | None:
    """
    Ім'я для відображення: first+last або @username. Повертає None, якщо нічого.
    """
    try:
        ch = await bot.get_chat(user_id)
        parts = []
        if getattr(ch, "first_name", None):
            parts.append(ch.first_name)
        if getattr(ch, "last_name", None):
            parts.append(ch.last_name)
        display = " ".join(parts).strip()
        if not display and getattr(ch, "username", None):
            display = f"@{ch.username}"
        return display or None
    except Exception:
        return None


async def _ensure_ticket_for_client(bot: Bot, client_id: int) -> dict:
    """
    Повертає відкритий тікет клієнта. Якщо відкритого немає —
    перевідкриває останній і пише в його тему. Якщо теми нема —
    створює нову. Для першого тікета: назва теми = ім'я клієнта з Telegram
    (або @username/ID), і одразу ставимо label = цьому імені.
    """
    # 1) відкритий/у роботі — використовуємо
    conn = get_conn()
    try:
        t = repo_t.find_open_by_client(conn, client_id)
    finally:
        conn.close()
    if t:
        return t

    # 2) відкритих немає → останній тікет
    conn = get_conn()
    try:
        last = repo_t.find_latest_by_client(conn, client_id)
    finally:
        conn.close()

    if last:
        thread_id = last.get("thread_id")

        # тема втрачена → створюємо нову з «людською» назвою (label/ім’я/ID)
        if not thread_id:
            display = await _client_display_name(bot, client_id)
            topic_name = (last.get("label") or display or f"ID{client_id}")[:128]
            topic = await bot.create_forum_topic(chat_id=settings.support_group_id, name=topic_name)

            conn = get_conn()
            try:
                repo_t.update_thread(conn, last["id"], topic.message_thread_id)
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

        # тема існує → просто reopen тікета
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

    # 3) ще не було тікетів → створюємо перший з іменем
    display = await _client_display_name(bot, client_id)
    topic_name = (display or f"ID{client_id}")[:128]
    topic = await bot.create_forum_topic(chat_id=settings.support_group_id, name=topic_name)

    conn = get_conn()
    try:
        ticket_id = repo_t.create(conn, client_id, topic.message_thread_id)
        if display:
            repo_t.set_label(conn, ticket_id, display)  # авто-label з імені
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

# -------------------- /start + телефон + кнопки --------------------

@router.message(Command("start"), F.chat.type == "private")
async def client_start(message: Message, bot: Bot):
    # якщо це співробітник — нічого не відповідаємо тут;
    # приватний /start для staff опрацьовує agents.py
    if await _is_staff_member(bot, message.from_user.id):
        return

    # клієнт: гарантуємо запис і дивимось, чи є телефон
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
        c = repo_c.get_client(conn, message.from_user.id)
        has_phone = bool(c and c.get("phone"))
        if not has_phone:
            # створити/оновити клієнта без номеру (як було у тебе)
            repo_c.upsert_client(conn, message.from_user.id, None, False)
    finally:
        conn.close()

    if has_phone:
        await message.answer("Головне меню:", reply_markup=main_menu_kb())
        await message.answer(WELCOME)
    else:
        await message.answer(PHONE_EXPLAIN, reply_markup=ask_phone_kb())
        await message.answer(
            "Натисніть кнопку нижче, щоб переглянути політику конфіденційності.",
            reply_markup=privacy_inline_kb(settings.PRIVACY_URL)
        )

@router.message(F.contact, F.chat.type == "private")
async def got_contact(message: Message):
    # фіксуємо клієнта в БД у будь-якому разі
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
    finally:
        conn.close()

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
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
    finally:
        conn.close()

    await message.answer("Добре, пропускаємо. Ви завжди зможете надіслати номер пізніше.", reply_markup=main_menu_kb())
    await message.answer(WELCOME)

# --------------- кнопки швидкого старту (і подія у тему) ---------------

@router.message(F.text == "🩺 Запитання по поточному лікуванню", F.chat.type == "private")
async def btn_current_treatment(message: Message, bot: Bot):
    # позначаємо «висячу» кнопку
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pp_client_intents (client_user_id, intent_label)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE intent_label=VALUES(intent_label), created_at=CURRENT_TIMESTAMP
            """, (message.from_user.id, "➡️ Кнопка: «🩺 Запитання по поточному лікуванню»"))
        conn.commit()
    finally:
        conn.close()

    await message.answer("Напишіть, будь ласка, ваше питання, і лікар відповість в найближчий час")

@router.message(F.text == "📅 Записатись на прийом", F.chat.type == "private")
async def btn_booking(message: Message, bot: Bot):
    # позначаємо «висячу» кнопку
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pp_client_intents (client_user_id, intent_label)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE intent_label=VALUES(intent_label), created_at=CURRENT_TIMESTAMP
            """, (message.from_user.id, "➡️ Кнопка: «📅 Записатись на прийом»"))
        conn.commit()
    finally:
        conn.close()

    await message.answer("Напишіть, будь ласка, зручний день/час, ім’я пацієнта та причину звернення (первинний огляд, вакцинації, діагностика тощо)")


@router.message(F.text == "❓ Задати питання", F.chat.type == "private")
async def btn_question(message: Message, bot: Bot):
    # позначаємо «висячу» кнопку
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pp_client_intents (client_user_id, intent_label)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE intent_label=VALUES(intent_label), created_at=CURRENT_TIMESTAMP
            """, (message.from_user.id, "➡️ Кнопка: «❓ Задати питання»"))
        conn.commit()
    finally:
        conn.close()

    await message.answer("Напишіть, будь ласка, зручний день/час та ім’я пацієнта.")

@router.message(F.text == "🗺 Як нас знайти", F.chat.type == "private")
async def btn_nav(message: Message, bot: Bot):
    # лиш гарантуємо, що клієнт є у БД — і все
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
    finally:
        conn.close()

    # Відповідь клієнту (без будь-якого логування в тему/групу)
    await message.answer(
        "📍 Київ, прт.Воскресенський 2/1 (Перова).\n"
        "🕒 Графік роботи — цілодобово\n"
        f"☎️ {settings.SUPPORT_PHONE}\n"
        "Google Maps: https://maps.app.goo.gl/Rir8Qgmzotz3RZMU7"
    )


# -------------------- клієнт → тема саппорт-групи --------------------

# ВАЖЛИВО: цей catch-all НЕ матчить команди.
# - бере текст, що НЕ починається з "/"
# - або будь-які повідомлення без тексту (фото/відео/док)
@router.message(
    F.chat.type == "private",
    (F.text & ~F.text.startswith("/")) | ~F.text
)
async def inbound_from_client(message: Message, bot: Bot):
    # staff ігноруємо
    if await _is_staff_member(bot, message.from_user.id):
        return

    # гарантований запис клієнта
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
    finally:
        conn.close()

    # знайти/створити тікет
    t = await _ensure_ticket_for_client(bot, message.from_user.id)
    label = t.get("label") or f"{message.from_user.id}"
    head = f"📨 Від клієнта <code>{label}</code>"

    # --- НОВЕ: підхоплюємо відкладений intent (кнопку)
    pending_intent = None
    conn = get_conn()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SELECT intent_label FROM pp_client_intents WHERE client_user_id=%s", (message.from_user.id,))
            row = cur.fetchone()
            if row:
                pending_intent = row["intent_label"]
                cur.execute("DELETE FROM pp_client_intents WHERE client_user_id=%s", (message.from_user.id,))
        conn.commit()
    finally:
        conn.close()

    # якщо є висяча кнопка — спочатку її лог у тему
    if pending_intent:
        await log_and_send_text_to_topic(
            bot, settings.support_group_id, t["thread_id"], t["id"], pending_intent, head
        )

    # далі — власне контент клієнта
    if message.content_type == "text":
        await log_and_send_text_to_topic(
            bot, settings.support_group_id, t["thread_id"], t["id"], message.text, head
        )
    else:
        await log_inbound_media_copy(
            message, settings.support_group_id, t["thread_id"], t["id"], head, bot
        )

@router.message(Command("menu"), F.chat.type == "private")
async def show_menu(message: Message):
    await message.answer("Головне меню:", reply_markup=main_menu_kb())
    await message.answer(WELCOME)
