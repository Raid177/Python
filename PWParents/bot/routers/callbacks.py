# bot/routers/ticket_callbacks.py

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from core.db import get_conn
from core.repositories import agents as repo_a
from core.repositories import tickets as repo_t

from bot.keyboards.common import assign_agents_kb, prefix_for_staff, ticket_actions_kb

router = Router()

# ---------- helpers ----------


def _parse(data: str):
    try:
        action, payload = data.split(":", 1)
        return action, payload
    except Exception:
        return None, None


def _abs_chat_id_str(chat_id: int) -> str:
    s = str(chat_id)
    if s.startswith("-100"):
        return s[4:]
    if s.startswith("-"):
        return s[1:]
    return s


async def build_topic_url(bot: Bot, group_id: int, thread_id: int) -> str:
    """
    Повертає прямий лінк на тему (forum topic) у службовій групі:
      - публічна група: https://t.me/<username>/<thread_id>
      - приватна група: https://t.me/c/<abs_chat_id>/<thread_id>
    """
    ch = await bot.get_chat(group_id)
    if getattr(ch, "username", None):  # публічна
        return f"https://t.me/{ch.username}/{thread_id}"
    return f"https://t.me/c/{_abs_chat_id_str(ch.id)}/{thread_id}"  # приватна


# ---------- callbacks ----------


@router.callback_query(F.data.startswith("pp."))
async def ticket_callbacks(cb: CallbackQuery, bot: Bot):
    action, payload = _parse(cb.data)
    if not action:
        return

    # Витягуємо тікет по клієнту/темі
    conn = get_conn()
    try:
        if action in ("pp.take", "pp.transfer", "pp.close"):
            client_id = int(payload)
            t = repo_t.find_latest_by_client(conn, client_id)
        elif action.startswith("pp.assignto"):
            # payload = "<client_id>:<assignee_id>"
            p1, p2 = payload.split(":")
            client_id = int(p1)
            assignee_id = int(p2)
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

    # --- Взяти в роботу (самопризначення) ---
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
                text=(
                    f"🟡 В роботі | Клієнт: <code>{t['label'] or client_id}</code>\n"
                    f"Виконавець: {who}"
                ),
                reply_markup=ticket_actions_kb(client_id),
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise

        await cb.answer("Взято в роботу")
        return

    # --- Показати список співробітників ---
    if action == "pp.transfer":
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
        return

    # --- Призначити конкретного виконавця ---
    if action.startswith("pp.assignto"):
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
                text=(
                    f"🟡 В роботі | Клієнт: <code>{t['label'] or client_id}</code>\n"
                    f"Виконавець: {who}"
                ),
                reply_markup=ticket_actions_kb(client_id),
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise

        # --- DM виконавцю: клікабельне посилання на тему ---
        try:
            topic_url = await build_topic_url(bot, cb.message.chat.id, t["thread_id"])
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="➡️ Відкрити тему", url=topic_url)]]
            )
            safe_label = str(t.get("label") or client_id)
            dm_text = f"🔔 Вам призначено звернення клієнта {safe_label}.\n{topic_url}"
            await bot.send_message(
                chat_id=assignee_id,
                text=dm_text,  # даємо сирий URL — клікається без parse_mode
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception:
            # мовчки ігноруємо, якщо немає /start у бота
            pass

        await cb.answer("Передано")
        return

    # --- Закрити ---
    if action == "pp.close":
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

        await bot.send_message(
            chat_id=t["client_user_id"],
            text="✅ Щиро дякуємо за довіру. Якщо знадобиться допомога — пишіть, будемо раді відповісти.",
        )
        await cb.answer("Закрито")
        return

    # --- Скасувати ---
    if action == "pp.cancel":
        await cb.answer("Скасовано")
        return
