# bot/routers/client.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import Contact, Message
from core.config import settings
from core.db import get_conn
from core.repositories import clients as repo_c
from core.repositories import tickets as repo_t
from core.services.relay import log_and_send_text_to_topic

from bot.keyboards.common import ask_phone_kb, main_menu_kb, privacy_inline_kb
from bot.routers._media import relay_media

router = Router()

WELCOME = (
    "Вітаємо у клініці PetWealth! 💚\n"
    f"Ми раді допомогти вам. Якщо питання термінове — телефонуйте {settings.SUPPORT_PHONE}.\n"
    "Зверніть увагу: чат не відслідковується постійно, але ми відповімо, щойно побачимо повідомлення."
)

PHONE_EXPLAIN = (
    "Щоб ми ідентифікували вас як власника тварини і надалі надсилали нагадування "
    "про візити/вакцинації, результати аналізів тощо — поділіться номером телефону.\n\n"
    "Це добровільно. Ви можете натиснути «Пропустити»."
)

ASK_PHONE_COOLDOWN = timedelta(hours=24)


# -------------------- helpers --------------------


def _touch_last_phone_prompt(conn, client_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pp_clients SET last_phone_prompt_at = UTC_TIMESTAMP() WHERE telegram_id = %s",
            (client_id,),
        )


def _should_prompt_phone(c: dict | None) -> bool:
    """
    Питаємо номер, якщо немає телефону і остання підказка була ≥ 24 год тому (або ніколи).
    """
    if not c:
        return True
    if c.get("phone"):
        return False

    last = c.get("last_phone_prompt_at")
    if not last:
        return True

    now = datetime.now(timezone.utc)
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (now - last) >= ASK_PHONE_COOLDOWN


async def _is_staff_member(bot: Bot, user_id: int) -> bool:
    try:
        cm = await bot.get_chat_member(settings.support_group_id, user_id)
        return cm.status in ("creator", "administrator", "member")
    except Exception:
        return False


async def _client_display_name(bot: Bot, user_id: int) -> str | None:
    """Ім'я для теми: first+last або @username, або None."""
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


async def _ensure_ticket_for_client(
    bot: Bot,
    client_id: int,
    *,
    silent: bool = False,
    notify_text: str | None = None,
) -> dict:
    """
    Повертає відкритий тікет; якщо нема — бере останній/створює новий і забезпечує тему.
    Поведінка повідомлень у групі:
      - silent=True  → нічого не шле;
      - silent=False + notify_text → шле лише notify_text (одне «м’яке» повідомлення);
      - silent=False + notify_text is None → шле стандартні «зелені» службові повідомлення.
    """
    # 1) Якщо є відкритий — використовуємо
    conn = get_conn()
    try:
        t = repo_t.find_open_by_client(conn, client_id)
    finally:
        conn.close()
    if t:
        return t

    # 2) Відкритих немає → беремо останній тікет
    conn = get_conn()
    try:
        last = repo_t.find_latest_by_client(conn, client_id)
    finally:
        conn.close()

    async def _notify(thread_id: int, label: str | int, status: str):
        if silent:
            return
        if notify_text:
            await bot.send_message(
                chat_id=settings.support_group_id, message_thread_id=thread_id, text=notify_text
            )
        else:
            await bot.send_message(
                chat_id=settings.support_group_id,
                message_thread_id=thread_id,
                text=f"🟢 Перевідкрито звернення клієнта <code>{label}</code>.",
            )
            await bot.send_message(
                chat_id=settings.support_group_id,
                message_thread_id=thread_id,
                text=(f"🟢 Заявка\nКлієнт: <code>{label}</code>\nСтатус: {status}"),
            )

    if last:
        thread_id = last.get("thread_id")

        # 2а) Тема втрачена → створюємо нову з людською назвою
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

            await _notify(last["thread_id"], last['label'] or client_id, last['status'])
            return last

        # 2б) Тема існує → просто reopen
        conn = get_conn()
        try:
            repo_t.reopen(conn, last["id"])
            last = repo_t.get_by_id(conn, last["id"])
        finally:
            conn.close()

        await _notify(last["thread_id"], last['label'] or client_id, last['status'])
        return last

    # 3) Ще не було тікетів → створюємо перший з іменем
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

    if not silent:
        if notify_text:
            await bot.send_message(
                chat_id=settings.support_group_id,
                message_thread_id=t["thread_id"],
                text=notify_text,
            )
        else:
            await bot.send_message(
                chat_id=settings.support_group_id,
                message_thread_id=t["thread_id"],
                text=(
                    f"🟢 Заявка\nКлієнт: <code>{t['label'] or t['client_user_id']}</code>\n"
                    f"Статус: {t['status']}"
                ),
            )
    return t


# -------------------- /start + телефон + кнопки --------------------


@router.message(Command("start"), F.chat.type == "private")
async def client_start(message: Message, bot: Bot):
    # Якщо це співробітник — показуємо службовий текст і виходимо
    try:
        cm = await bot.get_chat_member(settings.support_group_id, message.from_user.id)
        if cm.status in ("creator", "administrator", "member"):
            await message.answer(
                "Вітаю! Ви у команді PetWealth 🐾\n"
                "Для клієнтів працюємо у темах службової групи.\n"
                "Своє ім’я вкажіть у приваті: /setname Імʼя Прізвище."
            )
            return
    except Exception:
        pass

    # 1) гарантуємо запис клієнта
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
        c = repo_c.get_client(conn, message.from_user.id)  # dict або None
        if not c or not c.get("phone"):
            repo_c.upsert_client(conn, message.from_user.id, None, False)
    finally:
        conn.close()

    # 2) ОДРАЗУ забезпечуємо тікет/тему — клієнт видимий, але БЕЗ шуму
    await _ensure_ticket_for_client(
        bot,
        message.from_user.id,
        silent=True,  # ← тихо на /start
        # Можеш у майбутньому замінити на:
        # silent=False, notify_text="👋 Клієнт приєднався до бота"
    )

    # 3) Далі — телефон/меню
    if not c or not c.get("phone"):
        await message.answer(PHONE_EXPLAIN, reply_markup=ask_phone_kb())
        await message.answer(
            "Натисніть кнопку нижче, щоб переглянути політику конфіденційності.",
            reply_markup=privacy_inline_kb(settings.PRIVACY_URL),
        )
        conn = get_conn()
        try:
            _touch_last_phone_prompt(conn, message.from_user.id)
            conn.commit()
        finally:
            conn.close()
        return

    await message.answer("Головне меню:", reply_markup=main_menu_kb())
    await message.answer(WELCOME)


@router.message(F.contact, F.chat.type == "private", flags={"block": True})
async def got_contact(message: Message):
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
    finally:
        conn.close()

    contact: Contact = message.contact
    if not contact or not contact.phone_number:
        await message.answer(
            "Не вдалося отримати номер. Ви можете спробувати ще раз або натиснути «Пропустити».",
            reply_markup=ask_phone_kb(),
        )
        return

    conn = get_conn()
    try:
        repo_c.upsert_client(conn, message.from_user.id, contact.phone_number, True)
    finally:
        conn.close()

    await message.answer("Дякуємо! Номер збережено ✅", reply_markup=main_menu_kb())
    await message.answer(WELCOME)


@router.message(F.text == "➡️ Пропустити", F.chat.type == "private", flags={"block": True})
async def skip_phone(message: Message):
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
        _touch_last_phone_prompt(conn, message.from_user.id)
        conn.commit()
    finally:
        conn.close()

    await message.answer(
        "Добре, пропускаємо. Ви завжди зможете надіслати номер пізніше.",
        reply_markup=main_menu_kb(),
    )
    await message.answer(WELCOME)


# --------------- кнопки швидкого старту (зберігаємо «намір») ---------------


@router.message(
    F.text == "🩺 Запитання по поточному лікуванню", F.chat.type == "private", flags={"block": True}
)
async def btn_current_treatment(message: Message, bot: Bot):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pp_client_intents (client_user_id, intent_label)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE intent_label=VALUES(intent_label), created_at=CURRENT_TIMESTAMP
            """,
                (message.from_user.id, "➡️ Кнопка: «🩺 Запитання по поточному лікуванню»"),
            )
        conn.commit()
    finally:
        conn.close()

    await message.answer("Напишіть, будь ласка, ваше питання, і лікар відповість в найближчий час.")


@router.message(
    F.text == "📅 Записатись на прийом", F.chat.type == "private", flags={"block": True}
)
async def btn_booking(message: Message, bot: Bot):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pp_client_intents (client_user_id, intent_label)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE intent_label=VALUES(intent_label), created_at=CURRENT_TIMESTAMP
            """,
                (message.from_user.id, "➡️ Кнопка: «📅 Записатись на прийом»"),
            )
        conn.commit()
    finally:
        conn.close()

    await message.answer(
        "Напишіть зручний день/час, ім’я пацієнта та причину звернення (первинний огляд, вакцинація, діагностика тощо)."
    )


@router.message(F.text == "❓ Задати питання", F.chat.type == "private", flags={"block": True})
async def btn_question(message: Message, bot: Bot):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pp_client_intents (client_user_id, intent_label)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE intent_label=VALUES(intent_label), created_at=CURRENT_TIMESTAMP
            """,
                (message.from_user.id, "➡️ Кнопка: «❓ Задати питання»"),
            )
        conn.commit()
    finally:
        conn.close()

    await message.answer("Напишіть, будь ласка, ваше питання — і ми відповімо якнайшвидше.")


@router.message(F.text == "🗺 Як нас знайти", F.chat.type == "private", flags={"block": True})
async def btn_nav(message: Message, bot: Bot):
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
    finally:
        conn.close()

    await message.answer(
        "📍 Київ, прт.Воскресенський 2/1 (Перова).\n"
        "🕒 Графік роботи — цілодобово\n"
        f"☎️ {settings.SUPPORT_PHONE}\n"
        "Google Maps: https://maps.app.goo.gl/Rir8Qgmzotz3RZMU7"
    )


# -------------------- клієнт → тема саппорт-групи --------------------


@router.message(F.chat.type == "private", (F.text & ~F.text.startswith("/")) | ~F.text)
async def inbound_from_client(message: Message, bot: Bot):
    # 0) співробітників ігноруємо в клієнтському роутері
    if await _is_staff_member(bot, message.from_user.id):
        return

    # 1) гарантуємо запис у pp_clients і беремо рядок
    conn = get_conn()
    try:
        repo_c.ensure_exists(conn, message.from_user.id)
        client_row = repo_c.get_client(conn, message.from_user.id)
    finally:
        conn.close()

    # 2) ОДРАЗУ знайти/створити тікет і тему — клієнт видимий з першого пінгу
    t = await _ensure_ticket_for_client(
        bot,
        message.from_user.id,
        silent=False,  # тут хочемо бачити активність
        notify_text=None,  # або за бажанням коротко: "📩 Нове повідомлення від клієнта"
    )
    label = t.get("label") or f"{message.from_user.id}"
    head = f"📨 Від клієнта <code>{label}</code>"

    # 3) Якщо треба — попросити телефон, але НЕ зупиняти доставку у тему
    if _should_prompt_phone(client_row):
        await message.answer(PHONE_EXPLAIN, reply_markup=ask_phone_kb())
        await message.answer(
            "Натисніть кнопку нижче, щоб переглянути політику конфіденційності.",
            reply_markup=privacy_inline_kb(settings.PRIVACY_URL),
        )
        conn = get_conn()
        try:
            _touch_last_phone_prompt(conn, message.from_user.id)
            conn.commit()
        finally:
            conn.close()
        # без return → повідомлення все одно підемо в тему

    # 4) Підхопити «висячий» intent
    pending_intent = None
    conn = get_conn()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                "SELECT intent_label FROM pp_client_intents WHERE client_user_id=%s",
                (message.from_user.id,),
            )
            row = cur.fetchone()
            if row:
                pending_intent = row["intent_label"]
                cur.execute(
                    "DELETE FROM pp_client_intents WHERE client_user_id=%s", (message.from_user.id,)
                )
        conn.commit()
    finally:
        conn.close()

    if pending_intent:
        await log_and_send_text_to_topic(
            bot, settings.support_group_id, t["thread_id"], t["id"], pending_intent, head
        )

    # 5) Власне контент клієнта
    if message.content_type == "text":
        await log_and_send_text_to_topic(
            bot, settings.support_group_id, t["thread_id"], t["id"], message.text, head
        )
    else:
        out = await relay_media(
            bot,
            message,
            settings.support_group_id,
            prefix=head,
            thread_id=t["thread_id"],
        )
        try:
            from bot.service.msglog import log_and_touch

            log_and_touch(
                t["id"],
                "in",
                out.message_id,
                getattr(message, "caption", None),
                message.content_type,
            )
        except Exception:
            pass


# Додаткова команда, щоб руками викликати меню
@router.message(Command("menu"), F.chat.type == "private")
async def show_menu(message: Message):
    await message.answer("Головне меню:", reply_markup=main_menu_kb())
    await message.answer(WELCOME)


@router.message(Command("help"))
async def help_cmd(message: Message):
    await message.answer(
        "Доступні команди:\n"
        "/start — головне меню (тихо створює звернення у службовій групі)\n"
        "/menu — кнопки швидкого доступу\n"
        "/help — ця підказка\n\n"
        "У службовій групі доступні: /label, /assign, /card, /client, /phone, "
        "/threadinfo, /close, /close_silent, /reopen, /snooze."
    )
