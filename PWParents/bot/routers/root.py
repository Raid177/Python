# # bot/routers/root.py
# from aiogram import Router

# from . import agents, staff, callbacks, client  # ← порядок імпорту не критичний
#                                                 #   але порядок include — критичний

# router = Router(name="root")

# # ВАЖЛИВО: спочатку агентські/службові, потім клієнтські
# router.include_router(agents.router)
# router.include_router(staff.router)
# router.include_router(callbacks.router)
# router.include_router(client.router)


# bot/routers/root.py
from aiogram import Router
from . import client, agents, staff, callbacks

router = Router(name="root")

# Спочатку КЛІЄНТСЬКИЙ, потім службові
router.include_router(client.router)
router.include_router(agents.router)
router.include_router(staff.router)
router.include_router(callbacks.router)
