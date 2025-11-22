# bot/routers/health.py
import logging
import os
import platform
import time

import psutil
import requests
from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.auth import (
    ADMIN_ALERT_CHAT_ID,
    force_refresh,
    get_agent_info,
    is_allowed,
)
from core.config import settings
from core.db import get_conn

logger = logging.getLogger("health")

router = Router()
START_TS = time.monotonic()
UPDATE_COUNT = 0


def _fmt_seconds(sec: float) -> str:
    sec = int(sec)
    d, sec = divmod(sec, 86400)
    h, sec = divmod(sec, 3600)
    m, s = divmod(sec, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def _uptime_str() -> str:
    return _fmt_seconds(time.monotonic() - START_TS)


def _sys_load_cpu_mem():
    if hasattr(os, "getloadavg"):
        load = " / ".join(f"{x:.2f}" for x in os.getloadavg())
    else:
        load = "n/a"
    cpu = psutil.cpu_percent(interval=0.2)
    mem = psutil.Process().memory_info().rss / (1024 * 1024)
    return load, cpu, mem


@router.message(Command("status"))
async def cmd_status(message: Message):
    """
    /status — технічний статус бота:
      - версія, env, uptime
      - CPU, RAM, loadavg
      - ping до БД + базові метрики з БД
      - ping до Enote ODATA
    """
    # -------- DB health + лічильники --------
    db_ok = False
    db_err = ""
    total_clients = 0
    linked = 0
    open_tickets = 0

    t0 = time.perf_counter()
    conn = None

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            # Проста перевірка конекту
            cur.execute("SELECT 1")
            _ = cur.fetchone()  # обов'язково з’їдаємо результат

        # Далі вже метрики
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pp_clients")
            row = cur.fetchone()
            total_clients = row[0] if row else 0

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pp_clients WHERE owner_ref_key IS NOT NULL")
            row = cur.fetchone()
            linked = row[0] if row else 0

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pp_tickets WHERE status='open'")
            row = cur.fetchone()
            open_tickets = row[0] if row else 0

        db_ok = True
    except Exception as e:
        logger.exception("DB healthcheck failed")
        db_err = f"{type(e).__name__}: {e}"
    finally:
        if conn:
            conn.close()
    db_time = time.perf_counter() - t0

    # -------- Enote ODATA ping --------
    enote_ok = False
    enote_time = 0.0

    # Підтримуємо обидва варіанти імен (на випадок старого/нового config.py)
    enote_odata_url = getattr(settings, "ENOTE_ODATA_URL", None) or getattr(
        settings, "enote_odata_url", ""
    )
    enote_user = getattr(settings, "ENOTE_ODATA_USER", "") or getattr(
        settings, "enote_odata_user", ""
    )
    enote_pass = getattr(settings, "ENOTE_ODATA_PASS", "") or getattr(
        settings, "enote_odata_pass", ""
    )

    if enote_odata_url:
        try:
            t1 = time.perf_counter()
            # Проста перевірка: GET ...standard.odata/?$format=json
            test_url = f"{enote_odata_url.rstrip('/')}/?$format=json"
            r = requests.get(
                test_url,
                auth=(enote_user, enote_pass),
                timeout=5,
            )
            enote_ok = r.ok
            enote_time = time.perf_counter() - t1
        except Exception:
            enote_ok = False

    # -------- Системні метрики --------
    load, cpu, mem = _sys_load_cpu_mem()
    rel = f"  (released: {settings.APP_RELEASE})" if getattr(settings, "APP_RELEASE", "") else ""

    # env — з нового config.py (маленькими літерами)
    env = getattr(settings, "env", os.getenv("ENV", "unknown"))

    text = (
        "🐾 <b>PetWealth Parents Bot — STATUS</b>\n\n"
        f"version: <b>{settings.APP_VERSION}</b> (env: {env}){rel}\n"
        f"python: {platform.python_version()}\n"
        f"uptime: {_uptime_str()}\n"
        f"cpu: {cpu:.1f}%   mem: {mem:.1f} MB\n"
        f"loadavg: {load}\n\n"
        f"DB: {'✅ OK' if db_ok else '❌ ERR'} ({db_time:.2f}s)\n"
        + (f"<code>{db_err[:180]}</code>\n" if (not db_ok and db_err) else "")
        + f"Enote: {'✅ OK' if enote_ok else '❌ ERR'} ({enote_time:.2f}s)\n"
        + f"clients: {total_clients}   linked: {linked}\n"
        + f"tickets: {open_tickets} open\n"
        + f"updates seen: {UPDATE_COUNT}\n"
    )

    await message.answer(text)


@router.message(Command("ping"))
async def ping_cmd(message: Message, bot: Bot):
    if not await is_allowed(bot, message):
        return
    t0 = time.perf_counter()
    dt = (time.perf_counter() - t0) * 1000
    await message.reply(f"pong {dt:.1f} ms")


@router.message(Command("whoami"))
async def whoami_cmd(message: Message, bot: Bot):
    if not message.from_user:
        return

    allowed = await is_allowed(bot, message)
    if message.chat.id == ADMIN_ALERT_CHAT_ID:
        role = "admin-group"
    else:
        role = "allowed" if allowed else "client"

    info = await get_agent_info(message.from_user.id)
    if info:
        db_line = (
            f"display_name: {info['display_name']}\n"
            f"role: {info['role']}\n"
            f"active: {info['active']}"
        )
    else:
        db_line = "db: not found (pp_agents)"

    await message.reply(
        f"you are: {role}\n"
        f"user_id: {message.from_user.id}\n"
        f"chat_id: {message.chat.id}\n"
        f"{db_line}"
    )


@router.message(Command("version"))
async def version_cmd(message: Message, bot: Bot):
    if not await is_allowed(bot, message):
        return
    rel = f" (released {settings.APP_RELEASE})" if getattr(settings, "APP_RELEASE", "") else ""
    env = getattr(settings, "env", os.getenv("ENV", "unknown"))
    await message.reply(f"version: {settings.APP_VERSION}{rel} (env: {env})")


# Лічильник апдейтів — мідлвара
async def count_updates_middleware(handler, event, data):
    global UPDATE_COUNT
    UPDATE_COUNT += 1
    return await handler(event, data)


@router.message(Command("acl_reload"))
async def acl_reload_cmd(message: Message, bot: Bot):
    if message.chat.id != ADMIN_ALERT_CHAT_ID:
        return
    await force_refresh(bot)
    await message.reply("✅ ACL reloaded")


@router.message(Command("test"))
async def test_cmd(message: Message):
    await message.reply("✅ test ok")


@router.message(Command("boom"))
async def cmd_boom(message: Message):
    try:
        1 / 0
    except Exception:
        logging.getLogger("bot.test").exception("Штучна помилка для перевірки алерта")
        await message.answer("Згенерував помилку. Перевірте алерт у групі.")
