# bot/routers/root.py
from aiogram import Router
from . import agents, staff, callbacks, client

router = Router(name="root")

# Спочатку службові
router.include_router(agents.router)
router.include_router(staff.router)
router.include_router(callbacks.router)

# Клієнтський — останнім!
router.include_router(client.router)