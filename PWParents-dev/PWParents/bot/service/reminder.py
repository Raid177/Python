# bot/service/reminder.py
import asyncio
from datetime import datetime, timezone
from aiogram import Bot

from core.db import get_conn
from core.config import settings
from core.repositories import tickets as repo_t
from core.repositories import agents as repo_a
from bot.keyboards.common import assign_agents_kb


async def start_idle_reminder(bot: Bot):
    while True:
        try:
            await check_and_ping_assigned(bot)
            if settings.ESCALATE_UNASSIGNED:
                await check_and_escalate_unassigned(bot)
        except Exception:
            # не валимо цикл через одиничну помилку
            pass
        await asyncio.sleep(settings.REMINDER_PING_EVERY_MIN * 60)


async def check_and_ping_assigned(bot: Bot):
    """Пінгуємо відповідального, якщо клієнт чекає відповідь довше порогу.
    Поважаємо snooze: якщо snooze_until у майбутньому — не пінгуємо.
    """
    now_utc = datetime.now(timezone.utc).replace(
        tzinfo=None
    )  # порівнюємо naive з MySQL DATETIME

    conn = get_conn()
    try:
        tickets = repo_t.find_idle(
            conn, min_idle_minutes=settings.REMINDER_IDLE_MINUTES
        )
        # очікуємо, що find_idle вже відфільтровує закриті; якщо ні — підстрахуємося нижче
    finally:
        conn.close()

    for t in tickets:
        # 0) підстраховка: працюємо лише з open/in_progress
        if t.get("status") not in ("open", "in_progress"):
            continue

        # 1) snooze: якщо до цього часу «дрімає» — пропускаємо
        snooze_until = t.get("snooze_until")
        if snooze_until and snooze_until > now_utc:
            continue

        # 2) має бути виконавець
        if not t.get("assigned_to"):
            continue

        # 3) не частіше, ніж раз на REMINDER_PING_EVERY_MIN
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

            # опційно продублюємо в тему
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
    """Ескалація для неприкріплених заявок.
    Так само поважаємо snooze та статус тікета.
    """
    if not settings.ADMIN_ALERT_CHAT_ID:
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
        # 0) статус
        if t.get("status") not in ("open", "in_progress"):
            continue

        # 1) snooze
        snooze_until = t.get("snooze_until")
        if snooze_until and snooze_until > now_utc:
            continue

        # 2) не частіше, ніж раз на REMINDER_PING_EVERY_MIN
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
            # повідомлення в тему (якщо є)
            if t.get("thread_id"):
                try:
                    await bot.send_message(
                        chat_id=settings.support_group_id,
                        message_thread_id=t["thread_id"],
                        text=text,
                    )
                except Exception:
                    pass

            # алерт у адміністраторський чат
            await bot.send_message(
                chat_id=settings.ADMIN_ALERT_CHAT_ID, text=text, reply_markup=kb
            )

            # зафіксувати факт ескалації
            conn = get_conn()
            try:
                repo_t.mark_unassigned_alerted(conn, t["id"])
            finally:
                conn.close()

        except Exception:
            pass
