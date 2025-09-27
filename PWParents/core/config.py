from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    bot_token: str = os.getenv("BOT_TOKEN","")
    bot_username: str = os.getenv("BOT_USERNAME","")
    support_group_id: int = int(os.getenv("SUPPORT_GROUP_ID","0"))
    db_host: str = os.getenv("DB_HOST","127.0.0.1")
    db_port: int = int(os.getenv("DB_PORT","3306"))
    db_user: str = os.getenv("DB_USER","")
    db_password: str = os.getenv("DB_PASSWORD","")
    db_name: str = os.getenv("DB_NAME","")
    log_level: str = os.getenv("LOG_LEVEL","INFO")

settings = Settings()
