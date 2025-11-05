# bot/routers/root.py
from aiogram import Router
from . import agents, staff, callbacks, client, enote_link  # 👈 додали enote_link

router = Router(name="root")

# Спочатку службові
router.include_router(agents.router)
router.include_router(staff.router)
router.include_router(callbacks.router)
router.include_router(enote_link.router)  # 👈 вставили тут, до client

# Клієнтський — останнім!
router.include_router(client.router)
