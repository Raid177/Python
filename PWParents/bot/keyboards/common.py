# bot/keyboards/common.py

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from core.db import get_conn
from core.repositories.agents import get_display_name
from typing import Iterable

def prefix_for_staff(staff_tg_id:int) -> str:
    conn = get_conn()
    try:
        name = get_display_name(conn, staff_tg_id)
    finally:
        conn.close()
    label = name if name else f"ID{staff_tg_id}"
    return f"👩‍⚕️ {label}:"

def ticket_actions_kb(client_id:int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🟡 Взяти", callback_data=f"pp.take:{client_id}"),
        InlineKeyboardButton(text="🔁 Передати", callback_data=f"pp.transfer:{client_id}"),
        InlineKeyboardButton(text="🔴 Закрити", callback_data=f"pp.close:{client_id}")
    ]])

def assign_agents_kb(agents: Iterable[dict], client_id: int, exclude_id: int | None = None):
    # Кожен співробітник — окремий рядок кнопки
    rows = []
    for a in agents:
        if exclude_id and a["telegram_id"] == exclude_id:
            continue
        label = a["display_name"] or f"ID{a['telegram_id']}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"pp.assignto:{client_id}:{a['telegram_id']}")])
    # Кнопка «Скасувати»
    rows.append([InlineKeyboardButton(text="⬅️ Скасувати", callback_data=f"pp.cancel:{client_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)