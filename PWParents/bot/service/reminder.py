# bot/service/reminder.py
import asyncio
import logging
from datetime import datetime, timezone
from aiogram import Bot

from core.db import get_conn
from core.config import settings
from core.repositories import tickets as repo_t
from core.repositories import agents as repo_a
from bot.keyboards.common import assign_agents_kb

log = logging.getLogger("bot.service.reminder")


async def start_idle_reminder(bot: Bot):
    """
    Головний цикл нагадувань:
      - ping виконавцю, якщо клієнт чекає відповіді занадто довго
      - ескалація неприкріплених звернень (опційно)
    """
    log.info(
        "reminder: loop started (idle=%sm, ping=%sm, escalate=%s)",
        settings.REMINDER_IDLE_MINUTES,
        settings.REMINDER_PING_EVERY_MIN,
        settings.ESCALATE_UNASSIGNED,
    )
    while True:
        try:
            await check_and_ping_assigned(bot)
            if settings.ESCALATE_UNASSIGNED:
                await check_and_escalate_unassigned(bot)
        except Exception:
            # не ковтаємо тихо — щоб бачити реальні проблеми
            log.exception("reminder: loop error")
        await asyncio.sleep(settings.REMINDER_PING_EVERY_MIN * 60)


async def check_and_ping_assigned(bot: Bot):
    """
    Пінгуємо відповідального, якщо клієнт чекає відповідь довше порогу.
    Поважаємо snooze: якщо snooze_until у майбутньому — не пінгуємо.
    """
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)  # naive для MySQL DATETIME

    conn = get_conn()
    try:
        tickets = repo_t.find_idle(conn, min_idle_minutes=settings.REMINDER_IDLE_MINUTES)
    finally:
        conn.close()

    if tickets:
        log.info("reminder: candidates=%s ids=%s", len(tickets), [t["id"] for t in tickets])

    for t in tickets:
        # працюємо лише з open/in_progress
        if t.get("status") not in ("open", "in_progress"):
            continue

        # snooze → пропускаємо
        snooze_until = t.get("snooze_until")
        if snooze_until and snooze_until > now_utc:
            continue

        # має бути виконавець
        if not t.get("assigned_to"):
            continue

        # не частіше, ніж раз на REMINDER_PING_EVERY_MIN
        if (
            t.get("since_last_reminder") is not None
            and t["since_last_reminder"] < settings.REMINDER_PING_EVERY_MIN
        ):
            continue

        label = t.get("label") or str(t["client_user_id"])
        minutes = t.get("idle_minutes") or settings.REMINDER_IDLE_MINUTES

        try:
            # особистий пінг виконавцю
            await bot.send_message(
                chat_id=t["assigned_to"],
                text=(
                    f"⏰ Немає відповіді клієнту вже {minutes} хв у темі "
                    f"<code>{label}</code>. Будь ласка, відпишіться/передайте/закрийте."
                ),
            )

            # опційно — дубль у тему
            if settings.POST_ASSIGNED_REMINDER_TO_THREAD and t.get("thread_id"):
                try:
                    await bot.send_message(
                        chat_id=settings.support_group_id,
                        message_thread_id=t["thread_id"],
                        text=(
                            f"⚠️ Нагадування для виконавця <code>{label}</code>: "
                            f"клієнт чекає {minutes} хв. "
                            f"Будь ласка, відповідайте або передайте / закрийте."
                        ),
                    )
                except Exception:
                    # тему могли видалити — ігноруємо
                    pass

            # зафіксувати факт нагадування
            conn = get_conn()
            try:
                repo_t.mark_reminded(conn, t["id"])
            finally:
                conn.close()

        except Exception:
            # DM може не дійти (користувач не запускав бота тощо) — ігноруємо
            pass


async def check_and_escalate_unassigned(bot: Bot):
    """
    Ескалація для неприкріплених заявок.
    Поважаємо snooze та статус тікета.
    Відправляємо в адміністраторський чат/тред (якщо задано).
    """
    # поля в Settings — lower_case
    if not settings.admin_alert_chat_id:
        return

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_conn()
    try:
        tickets = repo_t.find_unassigned_idle(
            conn, min_idle_minutes=settings.UNASSIGNED_IDLE_MINUTES
        )
        agents = repo_a.list_active(conn)
    finally:
        conn.close()

    for t in tickets:
        # статус
        if t.get("status") not in ("open", "in_progress"):
            continue

        # snooze
        snooze_until = t.get("snooze_until")
        if snooze_until and snooze_until > now_utc:
            continue

        # не частіше, ніж раз на REMINDER_PING_EVERY_MIN
        if (
            t.get("since_last_alert") is not None
            and t["since_last_alert"] < settings.REMINDER_PING_EVERY_MIN
        ):
            continue

        label = t.get("label") or str(t["client_user_id"])
        minutes = t.get("idle_minutes") or settings.UNASSIGNED_IDLE_MINUTES
        kb = assign_agents_kb(agents, client_id=t["client_user_id"]) if agents else None

        text = (
            f"🚨 Первинне звернення без відповіді {minutes} хв.\n"
            f"Клієнт: <code>{label}</code>\nTicket ID: {t['id']}"
        )

        try:
            # повідомлення в тему клієнта (якщо є)
            if t.get("thread_id"):
                try:
                    await bot.send_message(
                        chat_id=settings.support_group_id,
                        message_thread_id=t["thread_id"],
                        text=text,
                    )
                except Exception:
                    pass

            # ескалація в адмін-чат (з урахуванням треду, якщо задано)
            send_kwargs = {
                "chat_id": settings.admin_alert_chat_id,
                "text": text,
                "reply_markup": kb,
            }
            if settings.admin_alert_thread_id:
                send_kwargs["message_thread_id"] = settings.admin_alert_thread_id

            await bot.send_message(**send_kwargs)

            # зафіксувати факт ескалації
            conn = get_conn()
            try:
                repo_t.mark_unassigned_alerted(conn, t["id"])
            finally:
                conn.close()

        except Exception:
            pass
