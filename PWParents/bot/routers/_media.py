# bot/routers/_media.py
from aiogram import Bot
from aiogram.types import Message

async def relay_media(
    bot: Bot,
    src_msg: Message,
    dst_chat_id: int,
    *,
    prefix: str | None = None,
    thread_id: int | None = None,
) -> Message:
    """
    Надійна передача будь-якого повідомлення з медіа/файлом у 'нативному' вигляді.
    - нічого не перезавантажуємо, просто копіюємо повідомлення (copy_message)
    - зберігається тип медіа, розмір, прев’ю, caption тощо
    - працює для photo/video/document/voice/video_note/audio/sticker тощо
    """
    # 1) опційна «шапка» перед медіа (для службового заголовка)
    if prefix:
        await bot.send_message(
            chat_id=dst_chat_id,
            message_thread_id=thread_id,
            text=prefix
        )

    # 2) власне копія повідомлення
    return await bot.copy_message(
        chat_id=dst_chat_id,
        from_chat_id=src_msg.chat.id,
        message_id=src_msg.message_id,
        message_thread_id=thread_id,
        caption=src_msg.caption,  # якщо був підпис — збережеться
    )
