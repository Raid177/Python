from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import CommandStart

from core.config import settings
from core.services.tickets import ensure_ticket
from core.services.relay import log_and_send_text_to_topic, log_inbound_media_copy

router = Router()

@router.message(CommandStart())
async def start_cmd(message: Message):
    await message.answer(
        "Вітаємо в PetWealth Parents! 🐾\n"
        "Надішліть своє питання тут — ми створимо (або знайдемо) вашу тему для команди.\n"
        "Надсилаючи повідомлення, ви погоджуєтесь із обробкою звернення в межах політики конфіденційності."
    )

@router.message(F.chat.type == "private")
async def inbound_from_client(message: Message, bot: Bot):
    t = await ensure_ticket(bot, settings.support_group_id, message.from_user.id)
    head = f"📨 Від клієнта <code>{message.from_user.id}</code>"
    if message.content_type == "text":
        await log_and_send_text_to_topic(bot, settings.support_group_id, t["thread_id"], t["id"], message.text, head)
    else:
        await log_inbound_media_copy(message, settings.support_group_id, t["thread_id"], t["id"], head, bot)
