# bot/service/cnt_visit_writer.py

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import List

import logging

from core.repositories.cnt_chat_repo import MessageRow
from bot.service.cnt_gpt_aggregator import aggregate_messages
from bot.service.cnt_enote_visit import VisitSostavData, build_sostav_payload
from bot.service.cnt_visit_repo import (
    find_recent_visit,
    create_new_visit,
    update_visit_sostav,
    post_visit,
)

log = logging.getLogger("cnt_visit_writer")


@dataclass
class VisitProcessResult:
    """
    Результат перенесення одного тікета у Document_Посещение.
    """
    visit_ref_key: str
    reused_existing: bool
    total_messages: int
    owner_block: str
    doctor_block: str
    changes_block: str


async def process_ticket_to_visit(
    messages: List[MessageRow],
    card_ref_key: str,
    agent_ref_key: str,
    ticket_closed_dt: datetime,
) -> VisitProcessResult:
    """
    Головний конвеєр:
    - на вхід даємо СУТО список текстових повідомлень тікета (MessageRow),
      ref_key картки тварини, ref_key лікаря та дату закриття тікета;
    - всередині:
        * GPT-агрегація
        * пошук/створення Document_Посещение
        * формування Состава
        * оновлення документа + проведення

    !!! УВАГА:
    Ця функція не лізе в БД та не знає про ticket_id.
    Вона працює тільки з готовими даними.
    """

    if not messages:
        raise ValueError("process_ticket_to_visit: список messages порожній")

    # 1) GPT-агрегація
    agg = await aggregate_messages(messages)
    log.info(
        "GPT агрегація завершена: %s повідомлень, len(owner)=%s, len(doctor)=%s, len(changes)=%s",
        agg.total_messages,
        len(agg.owner_block or ""),
        len(agg.doctor_block or ""),
        len(agg.changes_block or ""),
    )

    # 2) Шукаємо існуючий візит за останні VISIT_MERGE_WINDOW_HOURS
    existing = find_recent_visit(card_ref_key)
    reused_existing = False

    if existing and existing.get("Ref_Key"):
        visit_ref = existing["Ref_Key"]
        reused_existing = True
        log.info("Знайдено існуючий Document_Посещение Ref_Key=%s — будемо оновлювати", visit_ref)
    else:
        # 3) Створюємо новий візит
        visit_ref = create_new_visit(
            card_ref=card_ref_key,
            ticket_closed_dt=ticket_closed_dt,
            agent_ref_key=agent_ref_key,
        )
        if not visit_ref:
            raise RuntimeError("Не вдалося створити Document_Посещение")

        log.info("Створено новий Document_Посещение Ref_Key=%s", visit_ref)

    # 4) Формуємо Состав
    sostav = build_sostav_payload(
        VisitSostavData(
            visit_ref_key=visit_ref,
            owner_block=agg.owner_block,
            doctor_block=agg.doctor_block,
            changes_block=agg.changes_block,
            total_messages=agg.total_messages,
        )
    )

    # 5) PATCH Состава
    ok = update_visit_sostav(visit_ref, sostav)
    if not ok:
        raise RuntimeError(f"Не вдалося оновити Состав для Document_Посещение {visit_ref}")

    # 6) Проведення документа
    if not post_visit(visit_ref):
        # Не кидаємо виключення, але логнемо помилку
        log.error("Не вдалося провести Document_Посещение Ref_Key=%s", visit_ref)

    return VisitProcessResult(
        visit_ref_key=visit_ref,
        reused_existing=reused_existing,
        total_messages=agg.total_messages,
        owner_block=agg.owner_block,
        doctor_block=agg.doctor_block,
        changes_block=agg.changes_block,
    )
