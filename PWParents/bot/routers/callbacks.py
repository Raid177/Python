from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery
from aiogram.exceptions import TelegramBadRequest

from core.db import get_conn
from core.repositories import tickets as repo_t
from core.repositories import agents as repo_a
from bot.keyboards.common import ticket_actions_kb, prefix_for_staff, assign_agents_kb

router = Router()

def _parse(data:str):
    try:
        action, payload = data.split(":", 1)
        return action, payload
    except Exception:
        return None, None

@router.callback_query(F.data.startswith("pp."))
async def ticket_callbacks(cb: CallbackQuery, bot: Bot):
    action, payload = _parse(cb.data)
    if not action:
        return

    # Витягуємо клієнта/тікет
    conn = get_conn()
    try:
        if action in ("pp.take", "pp.transfer", "pp.close"):
            client_id = int(payload)
            t = repo_t.find_latest_by_client(conn, client_id)
        elif action.startswith("pp.assignto"):
            # payload = "<client_id>:<assignee_id>"
            p1, p2 = payload.split(":")
            client_id = int(p1); assignee_id = int(p2)
            t = repo_t.find_latest_by_client(conn, client_id)
        elif action.startswith("pp.cancel"):
            client_id = int(payload)
            t = repo_t.find_latest_by_client(conn, client_id)
        else:
            t = None
    finally:
        conn.close()

    if not t:
        await cb.answer("Заявку не знайдено", show_alert=True)
        return

    # --- Дії ---
    if action == "pp.take":
        conn = get_conn()
        try:
            repo_t.assign_to(conn, t["id"], cb.from_user.id)
            repo_t.set_status(conn, t["id"], "in_progress")
        finally:
            conn.close()
        who = prefix_for_staff(cb.from_user.id).replace("👩‍⚕️ ", "").replace(":", "")
        try:
            await bot.edit_message_text(
                chat_id=cb.message.chat.id,
                message_id=cb.message.message_id,
                text=(f"🟡 В роботі | Клієнт: <code>{t['label'] or client_id}</code>\n"
                      f"Виконавець: {who}"),
                reply_markup=ticket_actions_kb(client_id),
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
        await cb.answer("Взято в роботу")

    elif action == "pp.transfer":
        # Показати список співробітників для вибору (без негайної зміни картки)
        conn = get_conn()
        try:
            agents = repo_a.list_active(conn)
        finally:
            conn.close()

        kb = assign_agents_kb(agents, client_id, exclude_id=None)
        await bot.send_message(
            chat_id=cb.message.chat.id,
            message_thread_id=cb.message.message_thread_id,
            text=f"Кому передати клієнта <b>{t['label'] or client_id}</b>?",
            reply_markup=kb,
        )
        await cb.answer("Оберіть виконавця")

    elif action.startswith("pp.assignto"):
        # Призначити і повідомити лікаря у приват
        assignee_id = int(payload.split(":")[1])

        conn = get_conn()
        try:
            repo_t.assign_to(conn, t["id"], assignee_id)
            repo_t.set_status(conn, t["id"], "in_progress")
        finally:
            conn.close()

        who = prefix_for_staff(assignee_id).replace("👩‍⚕️ ", "").replace(":", "")
        try:
            await bot.edit_message_text(
                chat_id=cb.message.chat.id,
                message_id=cb.message.message_id,
                text=(f"🟡 В роботі | Клієнт: <code>{t['label'] or client_id}</code>\n"
                      f"Виконавець: {who}"),
                reply_markup=ticket_actions_kb(client_id),
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise

        # DM виконавцю
        try:
            await bot.send_message(
                chat_id=assignee_id,
                text=(f"🔔 Вам призначено звернення клієнта <b>{t['label'] or client_id}</b>.\n"
                      f"Зайдіть у тему в службовій групі й відповідайте від свого імені.")
            )
        except Exception:
            # мовчки ігноруємо, якщо немає /start у бота
            pass

        await cb.answer("Передано")

    elif action == "pp.close":
        conn = get_conn()
        try:
            repo_t.close_ticket(conn, t["id"])
        finally:
            conn.close()
        try:
            await bot.edit_message_text(
                chat_id=cb.message.chat.id,
                message_id=cb.message.message_id,
                text=(f"🔴 Закрито | Клієнт: <code>{t['label'] or client_id}</code>"),
                reply_markup=None,
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
        await bot.send_message(chat_id=t["client_user_id"],
                               text="✅ Щиро дякуємо за довіру. Якщо знадобиться допомога — пишіть, будемо раді відповісти.")
        await cb.answer("Закрито")

    elif action == "pp.cancel":
        await cb.answer("Скасовано")
