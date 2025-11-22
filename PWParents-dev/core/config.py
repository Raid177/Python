# core/config.py
from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv

# ------------------------------------------------------------------
# 1) Завантажуємо ОДИН .env із кореня проекту (PWParents-dev / PWParents)
# ------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]  # .../PWParents-dev
DOTENV = ROOT / ".env"
load_dotenv(DOTENV)


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
    # ----------------- Базовий режим -----------------
    env: str = os.getenv("ENV", "prod").lower()
    is_dev: bool = env == "dev"

    response_prefix: str = os.getenv(
        "RESPONSE_PREFIX",
        "[DEV]" if env == "dev" else ""
    )

    # ----------------- Telegram bot ------------------
    if env == "prod":
        bot_token: str = os.getenv("PROD_BOT_TOKEN", "")
        bot_username: str = os.getenv("PROD_BOT_USERNAME", "")
        support_group_id: int = _get_int("PROD_SUPPORT_GROUP_ID", 0)
        admin_alert_chat_id: int | None = _get_int_or_none("PROD_ADMIN_ALERT_CHAT_ID")
        admin_alert_thread_id: int | None = _get_int_or_none("PROD_ADMIN_ALERT_THREAD_ID")
    else:
        bot_token: str = os.getenv("DEV_BOT_TOKEN", "")
        bot_username: str = os.getenv("DEV_BOT_USERNAME", "")
        support_group_id: int = _get_int("DEV_SUPPORT_GROUP_ID", 0)
        admin_alert_chat_id: int | None = _get_int_or_none("DEV_ADMIN_ALERT_CHAT_ID")
        admin_alert_thread_id: int | None = _get_int_or_none("DEV_ADMIN_ALERT_THREAD_ID")

    # ----------------- DB ---------------------------
    db_host: str = os.getenv("DB_HOST", "127.0.0.1")
    db_port: int = _get_int("DB_PORT", 3306)
    db_user: str = os.getenv("DB_USER", "")
    db_password: str = os.getenv("DB_PASSWORD", "")
    db_name: str = (
        os.getenv("PROD_DB_NAME", "")
        if env == "prod"
        else os.getenv("DEV_DB_NAME", "")
    )

    # ----------------- Контакти / посилання ---------
    SUPPORT_PHONE: str = os.getenv("SUPPORT_PHONE", "(044) 33 44 55 1")
    PRIVACY_URL: str = os.getenv(
        "PRIVACY_URL",
        "https://docs.google.com/document/d/1ivMiBkzcg7hku9jmJFjrnlSSpJF83op2okRSOgU16I0/"
    )

    # ----------------- Логи / алерти ----------------
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    error_alerts_enabled: bool = _get_bool("ERROR_ALERTS_ENABLED", True)
    error_alert_min_level: str = os.getenv("ERROR_ALERT_MIN_LEVEL", "ERROR")

    # ----------------- SLA-нагадування --------------
    REMINDER_ENABLED: bool = _get_bool("REMINDER_ENABLED", True)
    REMINDER_IDLE_MINUTES: int = _get_int("REMINDER_IDLE_MINUTES", 30)
    REMINDER_PING_EVERY_MIN: int = _get_int("REMINDER_PING_EVERY_MIN", 10)

    ESCALATE_UNASSIGNED: bool = _get_bool("ESCALATE_UNASSIGNED", True)
    UNASSIGNED_IDLE_MINUTES: int = _get_int("UNASSIGNED_IDLE_MINUTES", 15)
    POST_ASSIGNED_REMINDER_TO_THREAD: bool = _get_bool(
        "POST_ASSIGNED_REMINDER_TO_THREAD", False
    )

    # ----------------- Phone reminder ---------------
    PHONE_REMINDER_ENABLED: bool = _get_bool("PHONE_REMINDER_ENABLED", True)
    PHONE_REMINDER_MIN_INTERVAL_MIN: int = _get_int("PHONE_REMINDER_MIN_INTERVAL_MIN", 720)
    PHONE_REMINDER_POLL_EVERY_MIN: int = _get_int("PHONE_REMINDER_POLL_EVERY_MIN", 10)
    PHONE_REMINDER_BATCH: int = _get_int("PHONE_REMINDER_BATCH", 50)

    # ----------------- Enote ODATA ------------------
    ENOTE_ODATA_USER: str = os.getenv("ENOTE_ODATA_USER", "")
    ENOTE_ODATA_PASS: str = os.getenv("ENOTE_ODATA_PASS", "")
    ENOTE_ODATA_URL: str = (
        os.getenv("PROD_ENOTE_ODATA_URL", "")
        if env == "prod"
        else os.getenv("DEV_ENOTE_ODATA_URL", "")
    ).rstrip("/")

    # ----------------- Enote API --------------------
    ENOTE_API_KEY: str = os.getenv("ENOTE_API_KEY", "")
    ENOTE_API_BASE: str = (
        os.getenv("PROD_ENOTE_API_BASE", "")
        if env == "prod"
        else os.getenv("DEV_ENOTE_API_BASE", "")
    ).rstrip("/")

    ENOTE_API_URL: str = (
        os.getenv("PROD_ENOTE_API_URL", "")
        if env == "prod"
        else os.getenv("DEV_ENOTE_API_URL", "")
    ).rstrip("/")

    # ----------------- OpenAI / GPT -----------------
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_ORG_ID: str = os.getenv("OPENAI_ORG_ID", "")
    OPENAI_PROJECT: str = os.getenv("OPENAI_PROJECT", "")
    OPENAI_API_BASE: str = os.getenv(
        "OPENAI_API_BASE",
        "https://api.openai.com/v1"
    ).rstrip("/")
    OPENAI_VISIT_MODEL: str = os.getenv("OPENAI_VISIT_MODEL", "gpt-4.1-mini")
    OPENAI_VISIT_TIMEOUT: int = _get_int("OPENAI_VISIT_TIMEOUT", 20)

    # ----------------- Visit-from-chat module -------
    VISIT_FEATURE_ENABLED: bool = _get_bool("VISIT_FEATURE_ENABLED", True)
    VISIT_MAX_MSG_PER_CHUNK: int = _get_int("VISIT_MAX_MSG_PER_CHUNK", 80)
    VISIT_MERGE_WINDOW_HOURS: int = _get_int("VISIT_MERGE_WINDOW_HOURS", 24)
    VISIT_LANGUAGE: str = os.getenv("VISIT_LANGUAGE", "uk")

    # Нові поля для роботи з Document_Посещение
    VISIT_ORG_KEY: str = os.getenv("VISIT_ORG_KEY", "")
    VISIT_DEPT_KEY: str = os.getenv("VISIT_DEPT_KEY", "")
    VISIT_COMMENT: str = os.getenv("VISIT_COMMENT", "Створено ботом PetWealth Parents")

    # ----------------- Версія / реліз ---------------
    APP_VERSION: str = os.getenv("APP_VERSION", "dev")
    APP_RELEASE: str = os.getenv("APP_RELEASE", "")


settings = Settings()
