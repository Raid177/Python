# core/config.py
from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    bot_token: str = os.getenv("BOT_TOKEN", "")
    bot_username: str = os.getenv("BOT_USERNAME", "")
    support_group_id: int = int(os.getenv("SUPPORT_GROUP_ID", "0"))

    db_host: str = os.getenv("DB_HOST", "127.0.0.1")
    db_port: int = int(os.getenv("DB_PORT", "3306"))
    db_user: str = os.getenv("DB_USER", "")
    db_password: str = os.getenv("DB_PASSWORD", "")
    db_name: str = os.getenv("DB_NAME", "")

    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # Нові поля
    SUPPORT_PHONE: str = os.getenv("SUPPORT_PHONE", "(044) 33 44 55 1")
    PRIVACY_URL: str = os.getenv("PRIVACY_URL", "https://wealth.pet")

    REMINDER_ENABLED: bool = os.getenv("REMINDER_ENABLED", "true").lower() in ("1","true","yes","on")
    REMINDER_IDLE_MINUTES: int = int(os.getenv("REMINDER_IDLE_MINUTES", "30"))
    REMINDER_PING_EVERY_MIN: int = int(os.getenv("REMINDER_PING_EVERY_MIN", "10"))

    ADMIN_ALERT_CHAT_ID: int = int(os.getenv("ADMIN_ALERT_CHAT_ID", "0"))  # ID групи адміністраторів (або приват чату чергового)
    ESCALATE_UNASSIGNED: bool = os.getenv("ESCALATE_UNASSIGNED", "true").lower() in ("1","true","yes","on")
    UNASSIGNED_IDLE_MINUTES: int = int(os.getenv("UNASSIGNED_IDLE_MINUTES", "15"))  # поріг для не призначених

    POST_ASSIGNED_REMINDER_TO_THREAD: bool = os.getenv("POST_ASSIGNED_REMINDER_TO_THREAD", "false").lower() in ("1","true","yes","on")



settings = Settings()
