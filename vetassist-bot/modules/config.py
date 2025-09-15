# config.py — читає токени/налаштування з /root/Python/_Acces/.env.prod
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv("/root/Python/_Acces/.env.prod", override=False)

@dataclass
class Settings:
    tg_bot_token: str = os.getenv("BOT_TOKEN_VetAssist", "")
    preprocess_mode: str = os.getenv("PREPROCESS_MODE", "lite").lower()

    # >>> ДОДАНО: доступ до OData Єнота <<<
    odata_url: str = os.getenv("ODATA_URL_COPY", "").rstrip("/")
    odata_user: str = os.getenv("ODATA_USER", "")
    odata_password: str = os.getenv("ODATA_PASSWORD", "")

    # Пошук пацієнта
    patient_entity: str = os.getenv("PATIENT_ENTITY", "Catalog_Карточки")
    patient_number_fields: tuple[str, ...] = ("НомерДоговора",)

settings = Settings()
