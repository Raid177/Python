from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery
from aiogram.exceptions import TelegramBadRequest

from core.db import get_conn
from core.repositories import tickets as repo_t
from bot.keyboards.common import ticket_actions_kb, prefix_for_staff

router = Router()

def _parse(data:str):
    try:
        action, client_id = data.split(":")
        return action, int(client_id)
    except Exception:
        return None, None

@router.callback_query(F.data.startswith("pp."))
async def ticket_callbacks(cb: CallbackQuery, bot: Bot):
    action, client_id = _parse(cb.data)
    if not action:
        return

    conn = get_conn()
    try:
        t = repo_t.find_latest_by_client(conn, client_id)
    finally:
        conn.close()
    if not t:
        await cb.answer("Заявку не знайдено", show_alert=True)
        return

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
        conn = get_conn()
        try:
            repo_t.assign_to(conn, t["id"], None)
            repo_t.set_status(conn, t["id"], "open")
        finally:
            conn.close()
        try:
            await bot.edit_message_text(
                chat_id=cb.message.chat.id,
                message_id=cb.message.message_id,
                text=(f"🟢 Вільно | Клієнт: <code>{t['label'] or client_id}</code>\n"
                      f"Натисніть «Взяти», щоб призначити виконавця"),
                reply_markup=ticket_actions_kb(client_id),
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
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
                               text="✅ Звернення закрито. Якщо потрібно — просто напишіть нове повідомлення.")
        await cb.answer("Закрито")
