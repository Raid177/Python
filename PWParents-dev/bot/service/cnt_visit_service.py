# bot/service/cnt_visit_service.py

from __future__ import annotations
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from core.repositories.cnt_chat_repo import get_ticket_messages
from core.repositories.tickets import get_ticket_by_id
from core.repositories.agents import get_agent_by_telegram
from core.repositories.cnt_visit_enote import (
    get_by_ticket_id,
    upsert_ticket_enote,
)
from bot.service.cnt_gpt_aggregator import aggregate_messages
from bot.service.cnt_visit_writer import process_ticket_to_visit

log = logging.getLogger("cnt_visit_service")


# ------------------------------------------------------
#  PREVIEW: тільки GPT, без Єнота
#  /visit_preview
# ------------------------------------------------------
async def process_ticket_preview(ticket_id: int,
                                 agent_telegram_id: int) -> Dict[str, Any]:
    """
    Агрегація переписки по тікету через GPT.
    НІЧОГО не створює в Єноті, просто повертає блоки тексту.

    Повертає dict:
      {
        "ok": bool,
        "error": str | None,
        "blocks": {
           "owner": str,
           "doctor": str,
           "changes": str,
           "total_messages": int,
        } | None
      }
    """

    # 1) Тікет
    ticket = get_ticket_by_id(ticket_id)
    if not ticket:
        return {
            "ok": False,
            "error": f"Тікет #{ticket_id} не знайдено.",
            "blocks": None,
        }

    # 2) Повідомлення
    msgs = get_ticket_messages(ticket_id)
    if not msgs:
        return {
            "ok": False,
            "error": f"У тікеті #{ticket_id} немає текстових повідомлень.",
            "blocks": None,
        }

    # 3) GPT-агрегація
    try:
        agg = await aggregate_messages(msgs)
    except Exception:
        log.exception("GPT aggregation error for ticket_id=%s", ticket_id)
        return {
            "ok": False,
            "error": "GPT-помилка під час агрегації переписки.",
            "blocks": None,
        }

    if not (agg.owner_block or agg.doctor_block or agg.changes_block):
        # GPT відповів, але порожньо
        return {
            "ok": False,
            "error": "GPT не повернув жодного блоку агрегації.",
            "blocks": None,
        }

    blocks = {
        "owner": agg.owner_block or "",
        "doctor": agg.doctor_block or "",
        "changes": agg.changes_block or "",
        "total_messages": agg.total_messages,
    }

    return {
        "ok": True,
        "error": None,
        "blocks": blocks,
    }


# ------------------------------------------------------
#  FULL PIPELINE: створення / оновлення Document_Посещение
#  /visit_create (або аналог)
# ------------------------------------------------------
async def process_ticket(ticket_id: int,
                         agent_telegram_id: int) -> Dict[str, Any]:
    """
    Повний цикл:
      - перевірки тікета
      - перевірка наявності card_ref (тварина)
      - пошук/створення Document_Посещение
      - GPT-агрегація
      - формування Состава
      - PATCH + Post документа
      - запис в pp_ticket_enote

    Повертає dict:
      {
        "ok": bool,
        "error": str | None,
        "visit_ref_key": str | None
      }
    """

    # 1) Тікет
    ticket = get_ticket_by_id(ticket_id)
    if not ticket:
        msg = f"Тікет #{ticket_id} не знайдено."
        log.error(msg)
        return {"ok": False, "error": msg, "visit_ref_key": None}

    # для створення реального візиту логічно вимагати closed_at
    closed_at: Optional[datetime] = ticket.get("closed_at")
    if not closed_at:
        msg = f"Тікет #{ticket_id} ще не закритий."
        log.error(msg)
        return {"ok": False, "error": msg, "visit_ref_key": None}

    # 2) pp_ticket_enote — тут повинні вже мати owner_ref_key + card_ref_key
    link = get_by_ticket_id(ticket_id)
    if not link or not link.card_ref_key:
        msg = f"Для тікета #{ticket_id} не вибрано тварину (card_ref_key)."
        log.error(msg)
        upsert_ticket_enote(
            ticket_id=ticket_id,
            card_ref_key=None,
            visit_ref_key=None,
            status="error",
            last_error="no_card_ref",
        )
        return {"ok": False, "error": msg, "visit_ref_key": None}

    card_ref = link.card_ref_key

    # 3) лікар (агент) → enote_ref_key
    agent = get_agent_by_telegram(agent_telegram_id)
    if not agent:
        msg = f"Агент telegram_id={agent_telegram_id} не знайдений у pp_agents."
        log.error(msg)
        upsert_ticket_enote(
            ticket_id=ticket_id,
            card_ref_key=card_ref,
            visit_ref_key=None,
            status="error",
            last_error="agent_not_found",
        )
        return {"ok": False, "error": msg, "visit_ref_key": None}

    enote_ref = agent.get("enote_ref_key")
    if not enote_ref:
        msg = f"Агент telegram_id={agent_telegram_id} не має enote_ref_key."
        log.error(msg)
        upsert_ticket_enote(
            ticket_id=ticket_id,
            card_ref_key=card_ref,
            visit_ref_key=None,
            status="error",
            last_error="no_agent_ref",
        )
        return {"ok": False, "error": msg, "visit_ref_key": None}

    # 4) Повідомлення
    msgs = get_ticket_messages(ticket_id)
    if not msgs:
        msg = f"У тікеті #{ticket_id} немає текстових повідомлень."
        log.error(msg)
        upsert_ticket_enote(
            ticket_id=ticket_id,
            card_ref_key=card_ref,
            visit_ref_key=None,
            status="error",
            last_error="no_messages",
        )
        return {"ok": False, "error": msg, "visit_ref_key": None}

    # 5) Основна магія — перенесення в Document_Посещение
    try:
        result = await process_ticket_to_visit(
            messages=msgs,
            card_ref_key=card_ref,
            agent_ref_key=enote_ref,
            ticket_closed_dt=closed_at,
        )
    except Exception as e:
        log.exception("Помилка process_ticket_to_visit для ticket_id=%s", ticket_id)
        upsert_ticket_enote(
            ticket_id=ticket_id,
            card_ref_key=card_ref,
            visit_ref_key=None,
            status="error",
            last_error=str(e),
        )
        return {
            "ok": False,
            "error": f"Помилка при створенні візиту: {e}",
            "visit_ref_key": None,
        }

    visit_ref = result.visit_ref_key

    # 6) Записуємо успіх у pp_ticket_enote
    upsert_ticket_enote(
        ticket_id=ticket_id,
        card_ref_key=card_ref,
        visit_ref_key=visit_ref,
        status="success",
        last_error=None,
    )

    log.info(
        "Успішно перенесено ticket_id=%s → Visit=%s",
        ticket_id,
        visit_ref,
    )

    return {
        "ok": True,
        "error": None,
        "visit_ref_key": visit_ref,
    }
