# bot/keyboards/common.py
from typing import Iterable

from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from core.db import get_conn
from core.repositories.agents import get_display_name, get_agent


def ask_phone_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поділитися номером", request_contact=True)],
            [KeyboardButton(text="➡️ Пропустити")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,  # ← було True
        # is_persistent=True        # ← якщо підтримує твій Bot API; інакше прибери
    )


def main_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🩺 Запитання по поточному лікуванню")],
            [KeyboardButton(text="📅 Записатись на прийом")],
            [KeyboardButton(text="❓ Задати питання")],
            [KeyboardButton(text="🗺 Як нас знайти")],
            [
                KeyboardButton(text="📱 Поділитись номером", request_contact=True)
            ],  # 🔹 ось ця
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        # is_persistent=True
    )


def privacy_inline_kb(url: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔒 Політика конфіденційності", url=url)]
        ]
    )


def prefix_for_staff(staff_tg_id: int, fallback: str | None = None) -> str:
    conn = get_conn()
    try:
        agent = get_agent(conn, staff_tg_id)
        name = (agent and agent.get("display_name")) or get_display_name(
            conn, staff_tg_id
        )
    finally:
        conn.close()

    base = name or fallback or f"ID{staff_tg_id}"

    role_suffix = ""
    if agent and agent.get("role"):
        role_suffix = f" _{agent['role']}_"  # підкреслення як просив

    label = f"{base}{role_suffix}"
    return f"👩‍⚕️ {label}:"


def ticket_actions_kb(client_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🟡 Взяти", callback_data=f"pp.take:{client_id}"
                ),
                InlineKeyboardButton(
                    text="🔁 Передати", callback_data=f"pp.transfer:{client_id}"
                ),
                InlineKeyboardButton(
                    text="🔴 Закрити", callback_data=f"pp.close:{client_id}"
                ),
            ]
        ]
    )


def assign_agents_kb(
    agents: Iterable[dict], client_id: int, exclude_id: int | None = None
):
    rows = []
    for a in agents:
        if exclude_id and a["telegram_id"] == exclude_id:
            continue
        label = a["display_name"] or f"ID{a['telegram_id']}"
        rows.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"pp.assignto:{client_id}:{a['telegram_id']}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Скасувати", callback_data=f"pp.cancel:{client_id}"
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)
