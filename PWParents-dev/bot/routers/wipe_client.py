import logging
from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

from core.config import settings
from core.db import get_conn
from bot.utils.staff_guard import IsSupportMember
from core.repositories import tickets as repo_t
from core.repositories import clients as repo_c

router = Router(name="wipe_client")
log = logging.getLogger(__name__)

def _kb_confirm(client_id: int, thread_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧨 ТАК, видалити назавжди", callback_data=f"wipe:ok:{client_id}:{thread_id}")],
        [InlineKeyboardButton(text="✋ Скасувати", callback_data="wipe:cancel")]
    ])

def _topic_summary(client_id: int, t_count: int, m_count: int, has_client: bool) -> str:
    return (
        f"• Клиент TG: <code>{client_id}</code>\n"
        f"• tickets: {t_count}\n"
        f"• messages: {m_count}\n"
        f"• pp_clients row: {'1' if has_client else '0'}\n"
    )

@router.message(
    Command("wipe_client_dry"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def wipe_client_dry(message: Message):
    """Показує, що буде видалено (без видалення)."""
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t:
            await message.answer("ℹ️ У цій темі немає активного тікета.")
            return
        client_id = t["client_user_id"]
        ticket_ids = repo_t.list_ids_by_client(conn, client_id)
        t_cnt = len(ticket_ids)
        m_cnt = repo_t.count_messages_by_ticket_ids(conn, ticket_ids)

        # чи існує клієнт у pp_clients
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pp_clients WHERE telegram_id=%s LIMIT 1", (client_id,))
            has_client = cur.fetchone() is not None

        text = (
            "🔎 <b>DRY-RUN</b> — що буде видалено\n" +
            _topic_summary(client_id, t_cnt, m_cnt, has_client)
        )
        await message.answer(text)
    finally:
        conn.close()

@router.message(
    Command("wipe_client"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def wipe_client_start(message: Message):
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t:
            await message.answer("ℹ️ У цій темі немає активного тікета.")
            return

        client_id = t["client_user_id"]
        thread_id = t["thread_id"]
        ticket_ids = repo_t.list_ids_by_client(conn, client_id)
        t_cnt = len(ticket_ids)
        m_cnt = repo_t.count_messages_by_ticket_ids(conn, ticket_ids)

        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pp_clients WHERE telegram_id=%s LIMIT 1", (client_id,))
            has_client = cur.fetchone() is not None
    finally:
        conn.close()

    warn = (
        "⚠️ <b>Підтвердіть видалення клієнта</b>\n"
        + _topic_summary(client_id, t_cnt, m_cnt, has_client)
        + "\nДія <u>незворотна</u>."
    )
    await message.answer(warn, reply_markup=_kb_confirm(client_id, thread_id))

@router.callback_query(F.data.startswith("wipe:"))
async def wipe_client_confirm(cb: CallbackQuery, bot: Bot):
    parts = cb.data.split(":", 3)
    if parts[1] == "cancel":
        try:
            await cb.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await cb.message.answer("Скасовано.")
        return

    if parts[1] != "ok" or len(parts) < 4:
        await cb.answer()
        return

    try:
        client_id = int(parts[2])
        thread_id = int(parts[3])
    except Exception:
        await cb.answer("Некоректні параметри.", show_alert=True)
        return

    # --- транзакційне видалення ---
    conn = get_conn()
    try:
        conn.start_transaction()
        ticket_ids = repo_t.list_ids_by_client(conn, client_id)
        m_deleted = repo_t.delete_messages_by_ticket_ids(conn, ticket_ids)
        t_deleted = repo_t.delete_tickets_by_client(conn, client_id)
        c_deleted = repo_c.delete_client(conn, client_id)
        conn.commit()

        log.info("wipe_client: client=%s messages=%s tickets=%s client_rows=%s",
                 client_id, m_deleted, t_deleted, c_deleted)

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass

        # у DEV показуємо причину помилки, щоб не бігати в консоль
        reason = str(e)
        await cb.message.answer(
            "❌ Помилка під час видалення з БД."
            + (f"\n<code>{reason}</code>" if settings.env == 'dev' else "")
        )
        log.exception("wipe_client failed | client=%s", client_id)
        return
    finally:
        conn.close()

    # прибираємо клавіатуру
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    # видалення/закриття теми
    deleted_topic = False
    try:
        await bot.delete_forum_topic(chat_id=settings.support_group_id, message_thread_id=thread_id)
        deleted_topic = True
    except TelegramBadRequest:
        try:
            await bot.close_forum_topic(chat_id=settings.support_group_id, message_thread_id=thread_id)
        except Exception:
            pass

    summary = (
        "🧹 <b>Клієнта очищено</b>\n"
        f"• Telegram ID: <code>{client_id}</code>\n"
        f"• Тема: {'видалена' if deleted_topic else 'закрита/залишилась'}\n"
        "• Дані: messages/tickets/pp_clients — видалено."
    )
    try:
        await bot.send_message(chat_id=settings.support_group_id, text=summary)
    except Exception:
        pass

    await cb.answer("Готово.")
