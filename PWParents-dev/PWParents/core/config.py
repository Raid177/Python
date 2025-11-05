# core/config.py
from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv

# --- load .env or .env.dev from project root (PWParents) ---
ROOT = Path(__file__).resolve().parents[1]  # .../PWParents
ENV = os.getenv("ENV", "prod").lower()
DOTENV = ROOT / (".env.dev" if ENV == "dev" else ".env")
load_dotenv(DOTENV)
# -----------------------------------------------------------

def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")

def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default

def _get_int_or_none(name: str):
    val = os.getenv(name, "").strip()
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        return None

@dataclass
class Settings:
    # --- Mode ---
    env: str = ENV
    is_dev: bool = (ENV == "dev")
    response_prefix: str = os.getenv("RESPONSE_PREFIX", "[DEV]" if ENV == "dev" else "")

    # --- Bot ---
    bot_token: str = os.getenv("BOT_TOKEN", "")
    bot_username: str = os.getenv("BOT_USERNAME", "")

    # --- Telegram groups/threads ---
    support_group_id: int = _get_int("SUPPORT_GROUP_ID", 0)
    admin_alert_chat_id: int | None = _get_int_or_none("ADMIN_ALERT_CHAT_ID")
    admin_alert_thread_id: int | None = _get_int_or_none("ADMIN_ALERT_THREAD_ID")

    # --- DB ---
    db_host: str = os.getenv("DB_HOST", "127.0.0.1")
    db_port: int = _get_int("DB_PORT", 3306)
    db_user: str = os.getenv("DB_USER", "")
    db_password: str = os.getenv("DB_PASSWORD", "")
    db_name: str = os.getenv("DB_NAME", "")

    # --- Misc/links ---
    SUPPORT_PHONE: str = os.getenv("SUPPORT_PHONE", "(044) 33 44 55 1")
    PRIVACY_URL: str = os.getenv("PRIVACY_URL", "https://wealth.pet")

    # --- Logging / alerts ---
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    error_alerts_enabled: bool = _get_bool("ERROR_ALERTS_ENABLED", True)
    error_alert_min_level: str = os.getenv("ERROR_ALERT_MIN_LEVEL", "ERROR")

    # --- Reminders / SLA ---
    REMINDER_ENABLED: bool = _get_bool("REMINDER_ENABLED", True)
    REMINDER_IDLE_MINUTES: int = _get_int("REMINDER_IDLE_MINUTES", 30)
    REMINDER_PING_EVERY_MIN: int = _get_int("REMINDER_PING_EVERY_MIN", 10)

    ESCALATE_UNASSIGNED: bool = _get_bool("ESCALATE_UNASSIGNED", True)
    UNASSIGNED_IDLE_MINUTES: int = _get_int("UNASSIGNED_IDLE_MINUTES", 15)
    POST_ASSIGNED_REMINDER_TO_THREAD: bool = _get_bool("POST_ASSIGNED_REMINDER_TO_THREAD", False)

settings = Settings()
