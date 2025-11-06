# bot/auth.py
import os
import time
import asyncio
from typing import Set, Optional

import aiomysql
from aiogram import Bot
from aiogram.types import Message
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest


ADMIN_ALERT_CHAT_ID = int(os.getenv("ADMIN_ALERT_CHAT_ID", "0"))
SUPPORT_GROUP_ID = int(os.getenv("SUPPORT_GROUP_ID", "0"))

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

ACL_REFRESH_SECONDS = int(os.getenv("ACL_REFRESH_SECONDS", "600"))  # 10 хв
AGENT_ROLES = tuple(
    r.strip()
    for r in os.getenv("AGENT_ROLES", "admin,agent,doctor").split(",")
    if r.strip()
)

# Увімкни/вимкни джерела
USE_ADMINS_FROM_ADMIN_GROUP = True
USE_ADMINS_FROM_SUPPORT_GROUP = False
USE_DB_AGENTS = True


class _AclCache:
    def __init__(self):
        self.agent_ids: Set[int] = set()
        self._last_refresh: float = 0.0
        self._lock = asyncio.Lock()

    def is_fresh(self) -> bool:
        return (time.time() - self._last_refresh) < ACL_REFRESH_SECONDS

    def set(self, ids: Set[int]):
        self.agent_ids = ids
        self._last_refresh = time.time()


_acl = _AclCache()


async def _fetch_group_admin_ids(bot: Bot, chat_id: int) -> Set[int]:
    ids: Set[int] = set()
    if not chat_id:
        return ids
    try:
        admins = await bot.get_chat_administrators(chat_id)
        for a in admins:
            if a.user and not a.user.is_bot:
                ids.add(a.user.id)
    except (TelegramForbiddenError, TelegramBadRequest):
        pass
    return ids


async def _fetch_db_agent_ids() -> Set[int]:
    """
    Даємо доступ усім із pp_agents, де active=1 і є telegram_id.
    """
    ids: Set[int] = set()
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        return ids

    conn: Optional[aiomysql.Connection] = None
    try:
        conn = await aiomysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            autocommit=True,
        )
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT DISTINCT telegram_id
                FROM pp_agents
                WHERE active = 1
                  AND telegram_id IS NOT NULL
            """)
            for (uid,) in await cur.fetchall():
                if uid:
                    ids.add(int(uid))
    except Exception:
        pass
    finally:
        if conn:
            conn.close()
    return ids


async def _refresh_acl(bot: Bot):
    ids: Set[int] = set()
    if USE_DB_AGENTS:
        ids |= await _fetch_db_agent_ids()
    if USE_ADMINS_FROM_ADMIN_GROUP and ADMIN_ALERT_CHAT_ID:
        ids |= await _fetch_group_admin_ids(bot, ADMIN_ALERT_CHAT_ID)
    if USE_ADMINS_FROM_SUPPORT_GROUP and SUPPORT_GROUP_ID:
        ids |= await _fetch_group_admin_ids(bot, SUPPORT_GROUP_ID)
    _acl.set(ids)


async def ensure_acl_fresh(bot: Bot):
    if _acl.is_fresh():
        return
    async with _acl._lock:
        if not _acl.is_fresh():
            await _refresh_acl(bot)


def allowed_in_private(user_id: int) -> bool:
    return user_id in _acl.agent_ids


def allowed_in_admin_group(chat_id: int) -> bool:
    return ADMIN_ALERT_CHAT_ID and chat_id == ADMIN_ALERT_CHAT_ID


async def is_allowed(bot: Bot, message: Message) -> bool:
    await ensure_acl_fresh(bot)
    if message.chat.type == "private" and message.from_user:
        return allowed_in_private(message.from_user.id)
    if allowed_in_admin_group(message.chat.id):
        return True
    return False


async def acl_refresher_task(bot: Bot):
    while True:
        try:
            await _refresh_acl(bot)
        except Exception:
            pass
        await asyncio.sleep(ACL_REFRESH_SECONDS)


# bot/auth.py (внизу файлу)
async def force_refresh(bot: Bot):
    await _refresh_acl(bot)


async def get_agent_info(user_id: int):
    """
    Повертає dict із полями display_name, role, active для pp_agents.telegram_id = user_id,
    або None якщо запису немає.
    """
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        return None

    conn = None
    try:
        conn = await aiomysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            autocommit=True,
        )
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT display_name, role, active
                FROM pp_agents
                WHERE telegram_id = %s
                LIMIT 1
            """,
                (user_id,),
            )
            row = await cur.fetchone()
            if row:
                display_name, role, active = row
                return {
                    "display_name": display_name,
                    "role": role,
                    "active": int(active),
                }
    except Exception:
        # не валимо команду, просто повертаємо None
        return None
    finally:
        if conn:
            conn.close()
    return None
