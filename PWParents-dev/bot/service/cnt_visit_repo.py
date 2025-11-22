# bot/service/cnt_visit_repo.py

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from core.config import settings
from core.integrations.enote import enote_request


log = logging.getLogger("cnt_visit_repo")

# GUID типу події "контроль"
CONTROL_EVENT_GUID = "445633dc-c3c7-11f0-846f-2ae983d8a0f0"
EMPTY_GUID = "00000000-0000-0000-0000-000000000000"


def find_recent_visit(card_ref: str) -> Optional[Dict[str, Any]]:
    """
    Пошук останнього контрольного Document_Посещение для цієї тварини (Карточки)
    за вікном settings.VISIT_MERGE_WINDOW_HOURS.
    """

    lookback = settings.VISIT_MERGE_WINDOW_HOURS
    dt_from = datetime.now() - timedelta(hours=lookback)
    dt_filter = dt_from.strftime("%Y-%m-%dT%H:%M:%S")

    url = (
        "Document_Посещение"
        f"?$filter=Карточка_Key eq guid'{card_ref}' "
        f"and ТипСобытия eq guid'{CONTROL_EVENT_GUID}' "
        f"and Date ge datetime'{dt_filter}'"
        "&$orderby=Date desc"
        "&$top=1"
    )

    try:
        data = enote_request("GET", url)
    except Exception:
        log.exception("Єнот не відповів при пошуку Document_Посещение")
        return None

    if not data or "value" not in data or not data["value"]:
        return None

    return data["value"][0]


def create_new_visit(
    card_ref: str,
    ticket_closed_dt: datetime,
    agent_ref_key: str
) -> Optional[str]:
    """
    Створення нового Document_Посещение (тільки шапка, без Состава).
    """

    payload = {
        "Date": ticket_closed_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "Карточка_Key": card_ref,
        "ТипСобытия": CONTROL_EVENT_GUID,

        # Значення з .env (універсальні для dev/prod)
        "Организация_Key": settings.VISIT_ORG_KEY,
        "Подразделение_Key": settings.VISIT_DEPT_KEY,
        "Комментарий": settings.VISIT_COMMENT,

        # Відповідальний та Автор — лікар, який вів переписку
        "Ответственный_Key": agent_ref_key,
        "Автор_Key": agent_ref_key,

        # службові поля
        "Назначение": "",
        "Основание_Key": EMPTY_GUID,
        "КонтактноеЛицо_Key": EMPTY_GUID,
        "ДисконтнаяКарточка_Key": EMPTY_GUID,
        "удалитьТоварныйЧек_Key": EMPTY_GUID,
        "ВрачРеферент_Key": EMPTY_GUID,
        "ID": "",

        # Состав додамо окремим PATCH
        "Состав": [],
    }

    try:
        created = enote_request("POST", "Document_Посещение", json=payload)
    except Exception:
        log.exception("Помилка створення нового Document_Посещение")
        return None

    if not created or "Ref_Key" not in created:
        log.error("Створено документ, але відсутній Ref_Key: %s", created)
        return None

    return created["Ref_Key"]


def update_visit_sostav(ref_key: str, sostav: list) -> bool:
    """
    Оновлення масиву 'Состав' у Document_Посещение.
    """

    payload = {"Состав": sostav}

    try:
        enote_request("PATCH", f"Document_Посещение(guid'{ref_key}')", json=payload)
    except Exception:
        log.exception("Помилка PATCH (Состав) у Document_Посещение")
        return False

    return True


def post_visit(ref_key: str) -> bool:
    """
    Проведення документа.
    """

    try:
        enote_request("POST", f"Document_Посещение(guid'{ref_key}')/Post")
    except Exception:
        log.exception("Помилка проведення Document_Посещение")
        return False

    return True
