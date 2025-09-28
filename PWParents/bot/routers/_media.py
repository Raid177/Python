from aiogram import Bot
from aiogram.types import Message

async def relay_media(bot: Bot, src: Message, dst_chat_id: int, prefix: str):
    await bot.send_message(chat_id=dst_chat_id, text=prefix)
    ct = src.content_type
    cap = getattr(src, "caption", None)

    if ct == "photo":
        return await bot.send_photo(dst_chat_id, src.photo[-1].file_id, caption=cap)
    if ct == "document":
        return await bot.send_document(dst_chat_id, src.document.file_id, caption=cap)
    if ct == "video":
        return await bot.send_video(dst_chat_id, src.video.file_id, caption=cap)
    if ct == "voice":
        return await bot.send_voice(dst_chat_id, src.voice.file_id, caption=cap)
    if ct == "audio":
        return await bot.send_audio(dst_chat_id, src.audio.file_id, caption=cap)
    if ct == "sticker":
        return await bot.send_sticker(dst_chat_id, src.sticker.file_id)
    if ct == "animation":
        return await bot.send_animation(dst_chat_id, src.animation.file_id, caption=cap)

    try:
        return await src.copy_to(chat_id=dst_chat_id)
    except Exception:
        return await bot.send_message(dst_chat_id, f"{prefix}\n\n(не вдалося переслати цей тип повідомлення)")
