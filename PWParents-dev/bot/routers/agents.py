# bot/routers/agents.py
import logging
import re  # для _clean_name

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.types import Message

from core.db import get_conn
from core.repositories import agents as repo_a
from core.repositories.agents import get_display_name
from core.config import settings  # лише для перевірки членства в групі

logger = logging.getLogger(__name__)
router = Router()


# ── helpers ─────────────────────────────────────────────────────────────────────

def _clean_name(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _is_active_agent(conn, telegram_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pp_agents WHERE telegram_id=%s AND active=1 LIMIT 1",
            (telegram_id,),
        )
        return cur.fetchone() is not None

async def _is_staff(bot: Bot, user_id: int) -> bool:
    """
    Дозволяємо команду, якщо користувач:
      1) є учасником саппорт-групи, або
      2) уже активний у БД (pp_agents.active=1).
    """
    try:
        cm = await bot.get_chat_member(settings.support_group_id, user_id)
        if cm.status in ("creator", "administrator", "member"):
            return True
    except TelegramBadRequest:
        pass

    conn = get_conn()
    try:
        return _is_active_agent(conn, user_id)
    finally:
        conn.close()


# ── /start (приватний лише для staff) ───────────────────────────────────────────

@router.message(
    StateFilter(None),
    F.chat.type == "private",
    Command("start"),
    flags={"block": False},
)
async def start_private(message: Message, bot: Bot):
    u = message.from_user

    if not await _is_staff(bot, u.id):
        # тихо ігноруємо сторонніх
        return

    # гарантуємо запис про агента
    conn = get_conn()
    try:
        repo_a.upsert_agent(conn, telegram_id=u.id, display_name="", role="doctor", active=1)
        conn.commit()

        # акуратно читаємо display_name (через той самий conn)
        display = get_display_name(conn, u.id)
    finally:
        conn.close()

    if display:
        await message.answer(
            f"Вітаю! Ти в команді PetWealth 🐾\n"
            f"• Твоє ім’я для клієнтів: <b>{display}</b>\n"
            f"• Твій Telegram ID: <a href='tg://user?id={u.id}'>{u.id}</a>\n"
            f"• У темі групи можна використовувати /assign, /label, /close тощо."
        )
    else:
        await message.answer(
            "Вітаю! Ти в команді PetWealth 🐾\n"
            "• Задай ім’я, яке бачитимуть клієнти: /setname Імʼя Прізвище\n"
            f"• Твій Telegram ID: <a href='tg://user?id={u.id}'>{u.id}</a>\n"
            "• У темі групи можна використовувати /assign, /label, /close тощо."
        )


# ── /setname (приватний лише для staff) ────────────────────────────────────────

@router.message(
    StateFilter(None),
    F.chat.type == "private",
    Command("setname"),
)
async def setname_private(message: Message, command: CommandObject, bot: Bot):
    uid = message.from_user.id

    if not await _is_staff(bot, uid):
        return  # стороннім нічого не відповідаємо

    # парсимо ім’я
    name = _clean_name(command.args or "")
    if not name:
        await message.answer("Використання: <code>/setname Імʼя Прізвище</code>")
        return
    if len(name) > 50:
        await message.answer("❌ Надто довге ім’я (макс 50 символів).")
        return

    # зберігаємо display_name
    conn = get_conn()
    try:
        # ВАРІАНТ A: якщо у тебе реалізований метод із activate=
        repo_a.set_display_name(conn, telegram_id=uid, display_name=name, activate=True)

        # ВАРІАНТ B (якщо без activate): спочатку гарантуємо запис, потім оновлюємо
        # repo_a.upsert_agent(conn, telegram_id=uid, display_name="", role="doctor", active=1)
        # repo_a.set_display_name(conn, telegram_id=uid, display_name=name)

        conn.commit()
    except Exception:
        logger.exception("setname failed")
        await message.answer("⚠️ Не вдалось оновити імʼя (БД). Спробуйте пізніше.")
        return
    finally:
        conn.close()

    await message.answer(f"Готово ✅ Ваше ім’я для клієнтів: <b>{name}</b>")
