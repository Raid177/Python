# bot/routers/root.py
from aiogram import Router
from . import enote_link, staff, callbacks, agents, client  # ← enote_link першим

router = Router(name="root")

# Спочатку службові, але щоб FSM не перебивав relay — enote_link вище за agents
router.include_router(enote_link.router)   # ← ПЕРШИМ
router.include_router(staff.router)
router.include_router(callbacks.router)
router.include_router(agents.router)       # ← ПІСЛЯ enote_link

# Клієнтський — останнім
router.include_router(client.router)
