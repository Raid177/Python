from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from core.db import get_conn
from core.repositories.agents import get_display_name

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
