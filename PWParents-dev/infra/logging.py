# infra/logging.py
import asyncio
import logging
import sys
from core.config import settings
from infra.telegram_log_handler import TelegramAlerter

LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def setup_logging(level_name: str = "INFO"):
    level = LEVELS.get(level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )

    # приглушити шум aiogram.event
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)

    # async unhandled exceptions -> у логи
    def _async_exc_handler(loop, context):
        logging.getLogger("asyncio").error(
            "Unhandled exception: %s",
            context.get("message"),
            exc_info=context.get("exception"),
        )

    try:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(_async_exc_handler)
    except Exception:
        pass

    # ---- Telegram алерти ----
    if settings.error_alerts_enabled:
        min_level = LEVELS.get(settings.error_alert_min_level.upper(), logging.ERROR)
        th = TelegramAlerter(
            bot_token=settings.bot_token,
            chat_id=settings.admin_alert_chat_id,
            thread_id=settings.admin_alert_thread_id,
            level=min_level,
        )
        # Красивий формат для TГ
        fmt = "<b>⚠️ {levelname}</b>\n<code>{name}</code>\n{message}"

        class _HtmlFormatter(logging.Formatter):
            def format(self, record):
                # додай traceback, якщо є
                base = fmt.format(
                    levelname=record.levelname,
                    name=record.name,
                    message=super().format(record),
                )
                if record.exc_info:
                    import traceback

                    tb = "".join(traceback.format_exception(*record.exc_info))[-3500:]
                    base += "\n\n<code>" + tb + "</code>"
                return base

        th.setFormatter(_HtmlFormatter("%(message)s"))
        logging.getLogger().addHandler(th)
