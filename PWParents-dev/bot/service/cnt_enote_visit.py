# bot/service/cnt_enote_visit.py

from __future__ import annotations
from typing import List, Dict
from dataclasses import dataclass


# GUID-и з твого прикладу Document_Посещение → Состав

# 1) Інформація від власника
Q_OWNER_VOPROS = "797be00c-c3c7-11f0-846f-2ae983d8a0f0"
Q_OWNER_SUB = "766ccdc2-c3c7-11f0-846f-2ae983d8a0f0"

# 2) Інформація від лікаря
Q_DOCTOR_VOPROS = "960b090a-c3c7-11f0-846f-2ae983d8a0f0"
Q_DOCTOR_SUB = "93ad0ee2-c3c7-11f0-846f-2ae983d8a0f0"

# 3) Зміни в лікуванні
Q_CHANGES_VOPROS = "abb0c3b2-c3c7-11f0-846f-2ae983d8a0f0"
Q_CHANGES_SUB = "a590209a-c3c7-11f0-846f-2ae983d8a0f0"

# 4) Кількість повідомлень (число)
Q_COUNT_VOPROS = "f8012932-c3c7-11f0-846f-2ae983d8a0f0"
Q_COUNT_SUB = "ee699ba2-c3c7-11f0-846f-2ae983d8a0f0"


@dataclass
class VisitSostavData:
    """
    Дані для формування Состав у Document_Посещение.
    """
    visit_ref_key: str
    owner_block: str
    doctor_block: str
    changes_block: str
    total_messages: int


def build_sostav_payload(data: VisitSostavData) -> List[Dict]:
    """
    Формує масив рядків Состав для Document_Посещение.

    На виході отримаємо щось типу:

    [
      { LineNumber=1, Вопрос_Key=Q_OWNER_*, ОткрытыйОтвет="...інфо від власника..." },
      { LineNumber=2, Вопрос_Key=Q_DOCTOR_*, ОткрытыйОтвет="...інфо від лікаря..." },
      { LineNumber=3, Вопрос_Key=Q_CHANGES_*, ОткрытыйОтвет="...зміни лікування..." },
      { LineNumber=4, Вопрос_Key=Q_COUNT_*, ОткрытыйОтвет="<N>", ТипОтвета="Число" },
    ]
    """
    ref = data.visit_ref_key

    lines: List[Dict] = []

    # 1. Власник
    lines.append({
        "Ref_Key": ref,
        "LineNumber": "1",
        "Вопрос_Key": Q_OWNER_VOPROS,
        "ЭлементарныйВопрос_Key": Q_OWNER_SUB,
        "НомерЯчейки": "0",
        "Ответ": "",
        "Ответ_Type": "StandardODATA.Undefined",
        "ОткрытыйОтвет": (data.owner_block or "").strip(),
        "ТипОтвета": "Текст",
    })

    # 2. Лікар
    lines.append({
        "Ref_Key": ref,
        "LineNumber": "2",
        "Вопрос_Key": Q_DOCTOR_VOPROS,
        "ЭлементарныйВопрос_Key": Q_DOCTOR_SUB,
        "НомерЯчейки": "0",
        "Ответ": "",
        "Ответ_Type": "StandardODATA.Undefined",
        "ОткрытыйОтвет": (data.doctor_block or "").strip(),
        "ТипОтвета": "Текст",
    })

    # 3. Зміни лікування
    lines.append({
        "Ref_Key": ref,
        "LineNumber": "3",
        "Вопрос_Key": Q_CHANGES_VOPROS,
        "ЭлементарныйВопрос_Key": Q_CHANGES_SUB,
        "НомерЯчейки": "0",
        "Ответ": "",
        "Ответ_Type": "StandardODATA.Undefined",
        "ОткрытыйОтвет": (data.changes_block or "").strip(),
        "ТипОтвета": "Текст",
    })

    # 4. Кількість повідомлень
    lines.append({
        "Ref_Key": ref,
        "LineNumber": "4",
        "Вопрос_Key": Q_COUNT_VOPROS,
        "ЭлементарныйВопрос_Key": Q_COUNT_SUB,
        "НомерЯчейки": "0",
        "Ответ": "",
        "Ответ_Type": "Edm.String",
        "ОткрытыйОтвет": str(int(data.total_messages or 0)),
        "ТипОтвета": "Число",
    })

    return lines
