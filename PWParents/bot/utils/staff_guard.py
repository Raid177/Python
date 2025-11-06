# bot/utils/staff_guard.py
from aiogram import Bot
from aiogram.filters import BaseFilter
from aiogram.types import Message
from core.config import settings

SUPPORT_STATUSES = {"creator", "administrator", "member"}


async def is_staff_member(bot: Bot, user_id: int) -> bool:
    """
    Перевіряє, що користувач є учасником службової групи (support_group_id).
    Повертає True лише для creator/administrator/member.
    """
    try:
        cm = await bot.get_chat_member(settings.support_group_id, user_id)
        return getattr(cm, "status", None) in SUPPORT_STATUSES
    except Exception:
        return False


class IsSupportMember(BaseFilter):
    """
    Фільтр для декораторів: спрацює лише якщо відправник є членом службової групи.
    """

    async def __call__(self, message: Message, bot: Bot) -> bool:
        if message.from_user is None:
            return False
        return await is_staff_member(bot, message.from_user.id)
