# bot/service/cnt_select_card.py

from __future__ import annotations
from typing import List
from dataclasses import dataclass

from aiogram import Bot, types
from aiogram.utils.keyboard import InlineKeyboardBuilder

from core.integrations import enote


@dataclass
class CardInfo:
    ref_key: str
    name: str


def fetch_cards_for_owner(owner_ref_key: str) -> List[CardInfo]:
    """
    Тягне список карток (тварин) для owner_ref_key через існуючу інтеграцію Enote.
    Використовує enote.odata_get_owner_cards(), яка повертає список dict із полями:
      - Ref_Key
      - Description
      - (інші, але нас цікавлять ці дві)
    """
    raw_cards = enote.odata_get_owner_cards(owner_ref_key) or []

    cards: List[CardInfo] = []
    for item in raw_cards:
        cards.append(
            CardInfo(
                ref_key=item.get("Ref_Key"),
                name=item.get("Description", ""),
            )
        )
    return cards


async def ask_card_selection(
    bot: Bot,
    chat_id: int,
    cards: List[CardInfo],
) -> CardInfo:
    """
    Якщо карток кілька — показує список кнопок і чекає вибір.
    Поки що тут заглушка: сам показ кнопок є, але очікування вибору
    ми реалізуємо у router (cnt_visit_router) окремим кроком.
    """

    if len(cards) == 1:
        return cards[0]

    kb = InlineKeyboardBuilder()
    for idx, c in enumerate(cards, start=1):
        kb.button(
            text=f"{idx}. {c.name}",
            callback_data=f"select_card:{c.ref_key}",
        )
    kb.adjust(1)

    await bot.send_message(
        chat_id,
        "Власник має кілька тварин.\nОберіть, до якої тварини стосується переписка:",
        reply_markup=kb.as_markup(),
    )

    # Тут ми ПРИНЦИПОВО не блокуємося і не ловимо callback.
    # Обробка callback'а буде в окремому router (cnt_visit_router),
    # який запише вибір у state / БД.
    raise NotImplementedError("Callback handler для вибору картки буде додано окремо.")


async def select_card_for_ticket(
    bot: Bot,
    chat_id: int,
    owner_ref_key: str,
) -> CardInfo:
    """
    High-level: отримати картки → якщо одна — повернути,
    якщо кілька — запустити ask_card_selection().
    """
    cards = fetch_cards_for_owner(owner_ref_key)

    if not cards:
        raise ValueError(f"У власника {owner_ref_key} не знайдено жодної картки")

    if len(cards) == 1:
        return cards[0]

    return await ask_card_selection(bot, chat_id, cards)
