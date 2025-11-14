# env_loader.py
from __future__ import annotations
import os
import re
import sys
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    print("Missing dependency: python-dotenv. Install with: pip install python-dotenv", file=sys.stderr)
    sys.exit(1)

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

@dataclass(frozen=True)
class Settings:
    # DB
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_database: str

    # Enote
    odata_user: str
    odata_password: str
    odata_url_prod: str
    odata_url_copy: str
    enote_env: str  # 'prod' | 'copy'
    enote_base_url: str  # resolved from env

    # Privat tokens (optional, any number via ENV)
    privat_tokens: dict

    # Behavior
    counterparty_refresh_before_run: bool
    counterparty_max_age_min: int
    path_enote_counterparty_script: str
    batch_limit: int
    pages_sleep_sec: float
    max_retries: int
    retry_backoff_sec: int
    dry_run: bool
    log_level: str
    log_file: Optional[str]

    # Defaults for Enote docs
    default_currency_guid_uah: str
    default_org_guid: str
    default_subdivision_guid: str
    default_responsible_guid: str


def _to_bool(v: str | None, default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

def _require(env: dict, key: str) -> str:
    val = env.get(key)
    if not val:
        raise RuntimeError(f"Missing required .env variable: {key}")
    return val

def _optional(env: dict, key: str, default=None):
    return env.get(key, default)

def _ensure_logging(log_level: str, log_file: Optional[str]) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.root.handlers.clear()
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logging.getLogger().addHandler(file_handler)

def _warn_if_not_uuid(val: str, name: str) -> None:
    if val and not UUID_RE.match(val):
        logging.getLogger("env").warning("Value %s looks non-UUID: %s", name, val)

def load_settings(dotenv_path: str | None = None) -> Settings:
    # 1) load .env
    load_dotenv(dotenv_path=dotenv_path)

    env = os.environ
    # 2) read required
    db_host = _require(env, "DB_HOST")
    db_port = int(_require(env, "DB_PORT"))
    db_user = _require(env, "DB_USER")
    db_password = _require(env, "DB_PASSWORD")
    db_database = _require(env, "DB_DATABASE")

    odata_user = _require(env, "ODATA_USER")
    odata_password = _require(env, "ODATA_PASSWORD")
    odata_url_prod = _require(env, "ODATA_URL")
    odata_url_copy = _require(env, "ODATA_URL_COPY")

    enote_env = _require(env, "ENOTE_ENV").strip().lower()
    if enote_env not in {"prod", "copy"}:
        raise RuntimeError("ENOTE_ENV must be 'prod' or 'copy'")
    enote_base_url = odata_url_prod if enote_env == "prod" else odata_url_copy

    # 3) optional/misc
    counterparty_refresh_before_run = _to_bool(_optional(env, "COUNTERPARTY_REFRESH_BEFORE_RUN", "1"))
    counterparty_max_age_min = int(_optional(env, "COUNTERPARTY_MAX_AGE_MIN", "1440"))
    path_enote_counterparty_script = _optional(env, "PATH_ENOTE_COUNTERPARTY_SCRIPT", "/root/Python/E-Note/et_x_Catalog_Контрагенты.py")

    batch_limit = int(_optional(env, "BATCH_LIMIT", "200"))
    pages_sleep_sec = float(_optional(env, "PAGES_SLEEP_SEC", "0.5"))
    max_retries = int(_optional(env, "MAX_RETRIES", "3"))
    retry_backoff_sec = int(_optional(env, "RETRY_BACKOFF_SEC", "5"))
    dry_run = _to_bool(_optional(env, "DRY_RUN", "0"))
    log_level = _optional(env, "LOG_LEVEL", "INFO")
    log_file = _optional(env, "LOG_FILE", None)

    default_currency_guid_uah = _require(env, "DEFAULT_CURRENCY_GUID_UAH")
    default_org_guid = _require(env, "DEFAULT_ORG_GUID")
    default_subdivision_guid = _require(env, "DEFAULT_SUBDIVISION_GUID")
    default_responsible_guid = _require(env, "DEFAULT_RESPONSIBLE_GUID")

    # 4) set logging early so warnings below are visible
    _ensure_logging(log_level, log_file)
    log = logging.getLogger("env")

    # 5) sanity checks
    if not enote_base_url.endswith("/"):
        log.warning("ODATA base URL does not end with '/': %s", enote_base_url)

    # file presence (only warn)
    if counterparty_refresh_before_run and not Path(path_enote_counterparty_script).exists():
        log.warning("Counterparty update script not found at PATH_ENOTE_COUNTERPARTY_SCRIPT: %s", path_enote_counterparty_script)

    # UUID warnings (do not fail)
    _warn_if_not_uuid(default_currency_guid_uah, "DEFAULT_CURRENCY_GUID_UAH")
    _warn_if_not_uuid(default_org_guid, "DEFAULT_ORG_GUID")
    _warn_if_not_uuid(default_subdivision_guid, "DEFAULT_SUBDIVISION_GUID")
    _warn_if_not_uuid(default_responsible_guid, "DEFAULT_RESPONSIBLE_GUID")

    # collect Privat tokens (any env var named PRIVAT_TOKEN_*)
    privat_tokens = {k.removeprefix("PRIVAT_TOKEN_"): v for k, v in env.items() if k.startswith("PRIVAT_TOKEN_") and v}

    # 6) build settings
    settings = Settings(
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_password=db_password,
        db_database=db_database,
        odata_user=odata_user,
        odata_password=odata_password,
        odata_url_prod=odata_url_prod,
        odata_url_copy=odata_url_copy,
        enote_env=enote_env,
        enote_base_url=enote_base_url,
        privat_tokens=privat_tokens,
        counterparty_refresh_before_run=counterparty_refresh_before_run,
        counterparty_max_age_min=counterparty_max_age_min,
        path_enote_counterparty_script=path_enote_counterparty_script,
        batch_limit=batch_limit,
        pages_sleep_sec=pages_sleep_sec,
        max_retries=max_retries,
        retry_backoff_sec=retry_backoff_sec,
        dry_run=dry_run,
        log_level=log_level,
        log_file=log_file,
        default_currency_guid_uah=default_currency_guid_uah,
        default_org_guid=default_org_guid,
        default_subdivision_guid=default_subdivision_guid,
        default_responsible_guid=default_responsible_guid,
    )

    # 7) summary to logs
    log.info("ENOTE_ENV=%s  -> %s", settings.enote_env, settings.enote_base_url)
    log.info("DB: %s@%s:%s/%s", settings.db_user, settings.db_host, settings.db_port, settings.db_database)
    log.info("Privat tokens loaded: %s", ", ".join(sorted(settings.privat_tokens.keys())) or "none")
    log.info("DRY_RUN=%s | BATCH_LIMIT=%s | MAX_RETRIES=%s", settings.dry_run, settings.batch_limit, settings.max_retries)
    return settings


if __name__ == "__main__":
    s = load_settings()
    print("--- SETTINGS LOADED ---")
    print(f"ENOTE_ENV={s.enote_env}  URL={s.enote_base_url}")
    print(f"DB={s.db_user}@{s.db_host}:{s.db_port}/{s.db_database}")
    print(f"Privat tokens: {', '.join(sorted(s.privat_tokens.keys())) or 'none'}")
