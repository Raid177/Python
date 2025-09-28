from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import CommandStart
from core.config import settings
from core.services.tickets import ensure_ticket
from core.services.relay import log_and_send_text_to_topic, log_inbound_media_copy

router = Router()

async def _is_member(bot: Bot, user_id: int) -> bool:
    try:
        cm = await bot.get_chat_member(settings.support_group_id, user_id)
        return cm.status in ("member", "administrator", "creator")
    except Exception:
        return False

@router.message(CommandStart())
async def start_cmd(message: Message, bot: Bot):
    # якщо користувач є в службовій групі — показуємо агентське вітання
    if await _is_member(bot, message.from_user.id):
        await message.answer(
            "Вітаю! Ти в команді PetWealth 🐾\n"
            "• Задай імʼя, яке бачитимуть клієнти: /setname Імʼя Прізвище\n"
            f"• Твій Telegram ID: <code>{message.from_user.id}</code>\n"
            "• У темі групи можна використовувати /assign, /label, /close тощо."
        )
        return

    # інакше — стандартне клієнтське привітання
    await message.answer(
        "Вітаємо в PetWealth Parents! 🐾\n"
        "Надішліть своє питання тут — ми створимо (або знайдемо) вашу тему для команди.\n"
        "Надсилаючи повідомлення, ви погоджуєтесь із політикою конфіденційності."
    )

@router.message(F.chat.type == "private")
async def inbound_from_client(message: Message, bot: Bot):
    # 1) не форвардимо команди клієнта (типу /setname, /assign тощо)
    if message.text and message.text.startswith("/"):
        return

    # 2) звичайний клієнтський потік
    t = await ensure_ticket(bot, settings.support_group_id, message.from_user.id)
    label = t.get("label") or f"{message.from_user.id}"
    head = f"📨 Від клієнта <code>{label}</code>"
    if message.content_type == "text":
        await log_and_send_text_to_topic(bot, settings.support_group_id, t["thread_id"], t["id"], message.text, head)
    else:
        await log_inbound_media_copy(message, settings.support_group_id, t["thread_id"], t["id"], head, bot)
