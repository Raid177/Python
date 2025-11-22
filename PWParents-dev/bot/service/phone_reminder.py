# bot/service/phone_reminder.py
import asyncio
import logging
from aiogram import Bot
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from core.config import settings
from core.db import get_conn

log = logging.getLogger(__name__)

def _ask_phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Поділитись телефоном", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

PHONE_PROMPT_TEXT = (
    "Щоб ми могли ідентифікувати вас як клієнта клініки, будь ласка, поділіться номером телефону."
)

async def _fetch_candidates(limit: int, min_interval_min: int) -> list[int]:
    """
    Вибирає клієнтів без телефону, яким можна нагадати (з урахуванням last_phone_prompt_at).
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_id
                FROM pp_clients
                WHERE (phone IS NULL OR phone = '')
                  AND (
                        last_phone_prompt_at IS NULL
                        OR last_phone_prompt_at < (UTC_TIMESTAMP() - INTERVAL %s MINUTE)
                      )
                ORDER BY COALESCE(last_phone_prompt_at, '1970-01-01')
                LIMIT %s
                """,
                (min_interval_min, limit),
            )
            rows = cur.fetchall()
            return [r[0] for r in rows] if rows else []
    finally:
        conn.close()

def _touch_last_prompt_bulk(ids: list[int]) -> None:
    if not ids:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # оновлюємо пачкою
            fmt = ",".join(["%s"] * len(ids))
            cur.execute(
                f"UPDATE pp_clients SET last_phone_prompt_at = UTC_TIMESTAMP() WHERE telegram_id IN ({fmt})",
                ids,
            )
        conn.commit()
    finally:
        conn.close()

async def start_phone_reminders(bot: Bot) -> None:
    """
    Фонова задача: періодично знаходить клієнтів без телефону і м’яко пінгує їх.
    """
    if not settings.PHONE_REMINDER_ENABLED:
        log.info("phone_reminder: disabled via env")
        return

    poll_sec = max(60, settings.PHONE_REMINDER_POLL_EVERY_MIN * 60)

    log.info(
        "phone_reminder: starting (interval=%sm, min_between_prompts=%sm, batch=%s)",
        settings.PHONE_REMINDER_POLL_EVERY_MIN,
        settings.PHONE_REMINDER_MIN_INTERVAL_MIN,
        settings.PHONE_REMINDER_BATCH,
    )

    # невелика затримка після старту бота
    await asyncio.sleep(5)

    while True:
        try:
            ids = await _fetch_candidates(
                limit=settings.PHONE_REMINDER_BATCH,
                min_interval_min=settings.PHONE_REMINDER_MIN_INTERVAL_MIN,
            )
            if ids:
                log.info("phone_reminder: candidates=%s", len(ids))
            sent_ok = []
            for uid in ids:
                try:
                    await bot.send_message(uid, PHONE_PROMPT_TEXT, reply_markup=_ask_phone_kb())
                    sent_ok.append(uid)
                except Exception as e:
                    # користувач міг не стартувати бота / заблокував — мовчки пропускаємо
                    log.debug("phone_reminder: skip uid=%s err=%s", uid, e)

            # щоб не спамити, відмічаємо, кому Наголосили (навіть якщо частині не доставилось)
            if sent_ok:
                _touch_last_prompt_bulk(sent_ok)

        except Exception:
            log.exception("phone_reminder: loop error")

        await asyncio.sleep(poll_sec)
