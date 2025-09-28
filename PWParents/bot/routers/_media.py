from aiogram import Bot
from aiogram.types import Message

async def relay_media(bot: Bot, src: Message, dst_chat_id: int, prefix: str):
    # Завжди надсилаємо префікс окремим повідомленням
    await bot.send_message(chat_id=dst_chat_id, text=prefix)
    ct = src.content_type
    cap = getattr(src, "caption", None)

    if ct == "photo":
        fid = src.photo[-1].file_id
        return await bot.send_photo(dst_chat_id, fid, caption=cap)
    if ct == "document":
        fid = src.document.file_id
        return await bot.send_document(dst_chat_id, fid, caption=cap)
    if ct == "video":
        fid = src.video.file_id
        return await bot.send_video(dst_chat_id, fid, caption=cap)
    if ct == "voice":
        fid = src.voice.file_id
        return await bot.send_voice(dst_chat_id, fid, caption=cap)
    if ct == "audio":
        fid = src.audio.file_id
        return await bot.send_audio(dst_chat_id, fid, caption=cap)
    if ct == "sticker":
        fid = src.sticker.file_id
        return await bot.send_sticker(dst_chat_id, fid)
    if ct == "animation":
        fid = src.animation.file_id
        return await bot.send_animation(dst_chat_id, fid, caption=cap)

    # Fallback: пробуємо copy_to або повідомляємо
    try:
        return await src.copy_to(chat_id=dst_chat_id)
    except Exception:
        return await bot.send_message(dst_chat_id, f"{prefix}\n\n(не вдалося переслати цей тип повідомлення)")
