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
from core.db import get_conn   # ← важливо: було відсутнє!

router = Router()
START_TS = time.monotonic()
UPDATE_COUNT = 0

def _fmt_seconds(sec: float) -> str:
    sec = int(sec)
    d, sec = divmod(sec, 86400)
    h, sec = divmod(sec, 3600)
    m, s = divmod(sec, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)

def _uptime_str() -> str:
    return _fmt_seconds(time.monotonic() - START_TS)

def _sys_load_cpu_mem():
    load = " / ".join(f"{x:.2f}" for x in os.getloadavg()) if hasattr(os, "getloadavg") else "n/a"
    cpu = psutil.cpu_percent(interval=0.2)
    mem = psutil.Process().memory_info().rss / (1024 * 1024)
    return load, cpu, mem

@router.message(Command("status"))
async def cmd_status(message: Message):
    # DB ping
    t0 = time.perf_counter()
    db_ok = False
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    finally:
        if conn:
            conn.close()
    db_time = time.perf_counter() - t0

        # Enote ping (OData перевірка без енкодингу кирилиці)
    enote_ok = False
    enote_time = 0.0
    if settings.ENOTE_ODATA_URL:
        try:
            t1 = time.perf_counter()
            # Беремо каталог із латиницею — наприклад, AccumulationRegister_Продажи_RecordType
            # або будь-який відомий, який точно існує
            test_url = f"{settings.ENOTE_ODATA_URL}/?$format=json"
            r = requests.get(
                test_url,
                auth=(settings.ENOTE_ODATA_USER, settings.ENOTE_ODATA_PASS),
                timeout=5,
            )
            enote_ok = r.ok
            enote_time = time.perf_counter() - t1
        except Exception as e:
            enote_ok = False

    # Лічильники з БД
    total_clients = linked = open_tickets = 0
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM pp_clients")
        total_clients = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM pp_clients WHERE owner_ref_key IS NOT NULL")
        linked = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM pp_tickets WHERE status='open'")
        open_tickets = cur.fetchone()[0]
    except Exception:
        pass
    finally:
        if conn:
            conn.close()

    load, cpu, mem = _sys_load_cpu_mem()
    rel = f"  (released: {settings.APP_RELEASE})" if settings.APP_RELEASE else ""
    text = (
        "🐾 <b>PetWealth Parents Bot — STATUS</b>\n\n"
        f"version: <b>{settings.APP_VERSION}</b> (env: {settings.ENV}){rel}\n"
        f"uptime: {_uptime_str()}\n"
        f"cpu: {cpu:.1f}%   mem: {mem:.1f} MB\n"
        f"loadavg: {load}\n\n"
        f"DB: {'✅ OK' if db_ok else '❌ ERR'} ({db_time:.2f}s)\n"
        f"Enote: {'✅ OK' if enote_ok else '❌ ERR'} ({enote_time:.2f}s)\n"
        f"clients: {total_clients}   linked: {linked}\n"
        f"tickets: {open_tickets} open\n"
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
    role = "admin-group" if (message.chat.id == ADMIN_ALERT_CHAT_ID) else ("allowed" if allowed else "client")
    info = await get_agent_info(message.from_user.id)
    if info:
        db_line = f"display_name: {info['display_name']}\nrole: {info['role']}\nactive: {info['active']}"
    else:
        db_line = "db: not found (pp_agents)"
    await message.reply(
        f"you are: {role}\nuser_id: {message.from_user.id}\nchat_id: {message.chat.id}\n{db_line}"
    )

@router.message(Command("version"))
async def version_cmd(message: Message, bot: Bot):
    if not await is_allowed(bot, message):
        return
    rel = f" (released {settings.APP_RELEASE})" if settings.APP_RELEASE else ""
    await message.reply(f"version: {settings.APP_VERSION}{rel}")

# лічильник апдейтів (як і було)
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
