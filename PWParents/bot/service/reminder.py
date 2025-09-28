#bot/service/reminder.py
import asyncio
from datetime import datetime, timezone
from aiogram import Bot
from core.db import get_conn
from core.config import settings
from core.repositories import tickets as repo_t
from bot.keyboards.common import assign_agents_kb  # опційно для швидкого призначення
from core.repositories import agents as repo_a     # щоб побудувати клавіатуру

async def start_idle_reminder(bot: Bot):
    while True:
        try:
            await check_and_ping_assigned(bot)
            if settings.ESCALATE_UNASSIGNED:
                await check_and_escalate_unassigned(bot)
        except Exception:
            pass
        await asyncio.sleep(settings.REMINDER_PING_EVERY_MIN * 60)

async def check_and_ping_assigned(bot: Bot):
    conn = get_conn()
    try:
        tickets = repo_t.find_idle(conn, min_idle_minutes=settings.REMINDER_IDLE_MINUTES)
    finally:
        conn.close()

    for t in tickets:
        if not t.get("assigned_to"):
            continue
        # не частіше, ніж раз на REMINDER_PING_EVERY_MIN
        if t.get("since_last_reminder") is not None and t["since_last_reminder"] < settings.REMINDER_PING_EVERY_MIN:
            continue
        label = t.get("label") or str(t["client_user_id"])
        minutes = t.get("idle_minutes") or settings.REMINDER_IDLE_MINUTES
        try:
            await bot.send_message(
                chat_id=t["assigned_to"],
                text=(f"⏰ Немає відповіді клієнту вже {minutes} хв у темі "
                      f"<code>{label}</code>. Будь ласка, відпишіться/передайте/закрийте.")
            )
            # опційно дублюємо в тему, щоб команда бачила «горить»
            if settings.POST_ASSIGNED_REMINDER_TO_THREAD and t.get("thread_id"):
                await bot.send_message(
                    chat_id=settings.support_group_id,
                    message_thread_id=t["thread_id"],
                    text=(f"⚠️ Нагадування для виконавця <code>{label}</code>: "
                          f"клієнт чекає {minutes} хв. "
                          f"Будь ласка, відповідайте або передайте / закрийте.")
                )
            conn = get_conn()
            try:
                repo_t.mark_reminded(conn, t["id"])
            finally:
                conn.close()
        except Exception:
            pass

async def check_and_escalate_unassigned(bot: Bot):
    if not settings.ADMIN_ALERT_CHAT_ID:
        return
    conn = get_conn()
    try:
        tickets = repo_t.find_unassigned_idle(conn, min_idle_minutes=settings.UNASSIGNED_IDLE_MINUTES)
        agents = repo_a.list_active(conn)
    finally:
        conn.close()

    for t in tickets:
        # не частіше, ніж раз на REMINDER_PING_EVERY_MIN
        if t.get("since_last_alert") is not None and t["since_last_alert"] < settings.REMINDER_PING_EVERY_MIN:
            continue
        label = t.get("label") or str(t["client_user_id"])
        minutes = t.get("idle_minutes") or settings.UNASSIGNED_IDLE_MINUTES
        kb = assign_agents_kb(agents, client_id=t["client_user_id"]) if agents else None
        text = (f"🚨 Первинне звернення без відповіді {minutes} хв.\n"
                f"Клієнт: <code>{label}</code>\nTicket ID: {t['id']}")
        try:
            if t.get("thread_id"):
                await bot.send_message(chat_id=settings.support_group_id, message_thread_id=t["thread_id"], text=text)
            await bot.send_message(chat_id=settings.ADMIN_ALERT_CHAT_ID, text=text, reply_markup=kb)
            conn = get_conn()
            try:
                repo_t.mark_unassigned_alerted(conn, t["id"])
            finally:
                conn.close()
        except Exception:
            pass
