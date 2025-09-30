# infra/telegram_log_handler.py
import asyncio
import logging
from aiogram import Bot
from aiogram.enums import ParseMode

class TelegramAlerter(logging.Handler):
    def __init__(self, bot_token: str, chat_id: int, thread_id: int | None = None,
                 level=logging.ERROR, throttle_seconds: int = 60):
        super().__init__(level)
        self._bot = Bot(bot_token)
        self._chat_id = chat_id
        self._thread_id = thread_id
        self._last = {}          # msg_hash -> ts
        self._throttle = throttle_seconds

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            # анти-спам по однакових повідомленнях
            key = (record.levelname, record.name, getattr(record, "msg", ""))
            loop = asyncio.get_running_loop()
            loop.create_task(self._send(msg))
        except Exception:
            # ніколи не валимо основний процес через лог-хендлер
            pass

    async def _send(self, text: str):
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text[:4096],  # захист від ліміту
                message_thread_id=self._thread_id,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception:
            # тихий фейл, щоб не створювати рекурсії логів
            pass
