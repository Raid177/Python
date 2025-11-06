# bot/routers/root.py
from aiogram import Router
from . import agents, callbacks, client, enote_link, staff  # ← enote_link першим
from . import wipe_client
from . import client_onboarding

router = Router(name="root")
router.include_router(client_onboarding.router)


# Спочатку службові, але щоб FSM не перебивав relay — enote_link вище за agents
router.include_router(enote_link.router)  # ← ПЕРШИМ
router.include_router(staff.router)
router.include_router(callbacks.router)
router.include_router(agents.router)  # ← ПІСЛЯ enote_link
router.include_router(wipe_client.router)  # ← додай

# Клієнтський — останнім
router.include_router(client.router)
