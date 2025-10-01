# bot/routers/health.py
import os, time, platform, psutil
from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.types import Message
from bot.auth import is_allowed, force_refresh, ADMIN_ALERT_CHAT_ID
from bot.auth import is_allowed, ADMIN_ALERT_CHAT_ID, get_agent_info  # ← додали get_agent_info
from aiogram import Router, F
import logging

router = Router()
START_TS = time.monotonic()
UPDATE_COUNT = 0
BOT_VERSION = os.getenv("BOT_VERSION", "dev")

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

@router.message(Command("status"))
async def status_cmd(message: Message, bot: Bot):
    if not await is_allowed(bot, message):
        return
    p = psutil.Process()
    mem = p.memory_info().rss / (1024**2)
    cpu = psutil.cpu_percent(interval=0.3)
    uptime = _fmt_seconds(time.monotonic() - START_TS)
    threads = p.num_threads()
    pid = p.pid
    loadavg = ""
    if hasattr(os, "getloadavg"):
        la = os.getloadavg()
        loadavg = f"\nloadavg: {la[0]:.2f} {la[1]:.2f} {la[2]:.2f}"
    await message.reply(
        "✅ Bot status\n"
        f"version: {BOT_VERSION}\n"
        f"uptime: {uptime}\n"
        f"updates: {UPDATE_COUNT}\n"
        f"pid: {pid}  threads: {threads}\n"
        f"cpu: {cpu:.1f}%  mem: {mem:.1f} MB\n"
        f"os: {platform.system()} {platform.release()} ({platform.machine()})"
        f"{loadavg}"
    )

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
        f"you are: {role}\n"
        f"user_id: {message.from_user.id}\n"
        f"chat_id: {message.chat.id}\n"
        f"{db_line}"
    )

@router.message(Command("version"))
async def version_cmd(message: Message, bot: Bot):
    if not await is_allowed(bot, message):
        return
    await message.reply(f"version: {BOT_VERSION}")

# middleware-лічильник
async def count_updates_middleware(handler, event, data):
    global UPDATE_COUNT
    UPDATE_COUNT += 1
    return await handler(event, data)

@router.message(Command("acl_reload"))
async def acl_reload_cmd(message: Message, bot: Bot):
    # дозволяємо лише з адмін-групи
    if message.chat.id != ADMIN_ALERT_CHAT_ID:
        return
    await force_refresh(bot)
    await message.reply("✅ ACL reloaded")

@router.message(Command("test"))
async def test_cmd(message: Message):
    await message.reply("✅ test ok")

@router.message(Command("boom"))
async def cmd_boom(message: Message):
    # імітація помилки у коді
    try:
        1/0
    except Exception:
        logging.getLogger("bot.test").exception("Штучна помилка для перевірки алерта")
        await message.answer("Згенерував помилку. Перевірте алерт у групі.")
