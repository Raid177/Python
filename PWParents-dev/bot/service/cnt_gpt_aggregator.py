# bot/service/cnt_gpt_aggregator.py

from __future__ import annotations
from typing import List, Tuple, Dict
from dataclasses import dataclass

import httpx

from core.config import settings
from core.repositories.cnt_chat_repo import MessageRow


@dataclass
class AggregationResult:
    owner_block: str
    doctor_block: str
    changes_block: str
    total_messages: int


# ---------------------------------------------------------
#   PROMPTS
# ---------------------------------------------------------

SYSTEM_PROMPT = """
Ви — професійний асистент ветеринарного лікаря.
Ваше завдання — коротко агрегувати переписку між власником і лікарем
в форматі трьох блоків:

1. Інформація від власника
2. Інформація від лікаря
3. Зміни в лікуванні (якщо вони були)

Не переписуйте дослівно, не дублюйте повідомлення.
Компресуйте зміст до коротких, інформативних тез.
Мова відповіді — українська.
"""


CHUNK_PROMPT = """
Зробіть коротку агреговану вижимку для цього фрагмента переписки.
Формат відповіді — JSON:

{
  "owner": "...",
  "doctor": "...",
  "treatment": "..."
}

Не додавайте пояснень.
"""


MERGE_PROMPT = """
У вас є кілька локальних агрегованих блоків.
Надішлю їх нижче у JSON-форматі.

Потрібно виконати фінальне об’єднання, склавши три глобальні блоки:
- інформація від власника
- інформація від лікаря
- зміни в лікуванні

Не дублюйте одне й те саме. Агрегуйте зміст.
Мова відповіді — українська.

Формат відповіді (JSON):

{
  "owner": "...",
  "doctor": "...",
  "treatment": "..."
}
"""


# ---------------------------------------------------------
#   GPT API helper
# ---------------------------------------------------------

async def _call_openai(messages: List[Dict]) -> str:
    """
    Внутрішній низькорівневий виклик OpenAI ChatCompletions.
    Повертає raw-text відповіді.
    """
    headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    if settings.OPENAI_ORG_ID:
        headers["OpenAI-Organization"] = settings.OPENAI_ORG_ID
    if settings.OPENAI_PROJECT:
        headers["OpenAI-Project"] = settings.OPENAI_PROJECT

    body = {
        "model": settings.OPENAI_VISIT_MODEL,
        "messages": messages,
        "temperature": 0.0,
    }

    timeout = settings.OPENAI_VISIT_TIMEOUT

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(
            f"{settings.OPENAI_API_BASE}/chat/completions",
            headers=headers,
            json=body,
        )
        r.raise_for_status()
        data = r.json()

    return data["choices"][0]["message"]["content"]


# ---------------------------------------------------------
#   Aggregate single chunk
# ---------------------------------------------------------

def _format_chunk(messages: List[MessageRow]) -> str:
    """
    Перетворює список MessageRow у простий текст:
    'owner: ...' / 'doctor: ...'
    """
    lines = []
    for m in messages:
        if not m.text:
            continue

        if m.direction == "in":
            prefix = "owner"
        else:
            prefix = "doctor"

        lines.append(f"{prefix}: {m.text}")

    return "\n".join(lines)


async def _aggregate_chunk(messages: List[MessageRow]) -> Dict[str, str]:
    """
    Агрегація одного чанку.
    Повертає JSON:
    {
        "owner": "...",
        "doctor": "...",
        "treatment": "..."
    }

    Якщо GPT не дотримався формату JSON, намагаємось видерти JSON із тексту.
    Якщо і це не вдається — кладемо весь текст у "owner".
    """
    import json
    import re
    import logging

    text = _format_chunk(messages)
    log = logging.getLogger("cnt_gpt_aggregator")

    if not text.strip():
        return {"owner": "", "doctor": "", "treatment": ""}

    raw = await _call_openai([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": CHUNK_PROMPT + "\n\n" + text},
    ])

    log.debug("GPT raw chunk response: %r", raw)

    owner = ""
    doctor = ""
    treatment = ""

    try:
        # 1) Пробуємо знайти JSON-об'єкт у відповіді
        m = re.search(r"\{.*\}", raw, re.S)
        json_str = m.group(0) if m else raw

        data = json.loads(json_str)

        owner = str(data.get("owner", "") or "").strip()
        doctor = str(data.get("doctor", "") or "").strip()
        treatment = str(data.get("treatment", "") or "").strip()

    except Exception as e:
        # Якщо парсинг не вдався — хоч щось зберігаємо
        log.warning("Failed to parse GPT chunk JSON, using plain text fallback: %s", e)
        owner = raw.strip()

    return {
        "owner": owner,
        "doctor": doctor,
        "treatment": treatment,
    }


# ---------------------------------------------------------
#   Hierarchical merge of chunk summaries
# ---------------------------------------------------------

async def _merge_chunks(chunks: List[Dict[str, str]]) -> Dict[str, str]:
    """
    chunks — список:
    [
        {"owner": "...", "doctor": "...", "treatment": "..."},
        ...
    ]
    """

    import json
    raw_input = json.dumps(chunks, ensure_ascii=False, indent=2)

    raw = await _call_openai([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": MERGE_PROMPT + "\n\n" + raw_input},
    ])

    try:
        data = json.loads(raw)
    except:
        data = {"owner": "", "doctor": "", "treatment": ""}

    return {
        "owner": data.get("owner", ""),
        "doctor": data.get("doctor", ""),
        "treatment": data.get("treatment", ""),
    }


# ---------------------------------------------------------
#   Public API — MAIN AGGREGATOR
# ---------------------------------------------------------

async def aggregate_messages(messages: List[MessageRow]) -> AggregationResult:
    """
    Головний метод:
    - розбиває messages на чанки по settings.VISIT_MAX_MSG_PER_CHUNK
    - кожен чанк агрегує
    - після цього викликає фінальний merge
    - повертає AggregationResult
    """

    max_n = settings.VISIT_MAX_MSG_PER_CHUNK

    # Розбиваємо на чанки
    chunks: List[List[MessageRow]] = []
    current = []

    for m in messages:
        current.append(m)
        if len(current) >= max_n:
            chunks.append(current)
            current = []

    if current:
        chunks.append(current)

    # Якщо всього 1 чанк — можна зекономити виклик
    chunk_summaries = []
    for ch in chunks:
        summary = await _aggregate_chunk(ch)
        chunk_summaries.append(summary)

    if len(chunk_summaries) == 1:
        merged = chunk_summaries[0]
    else:
        merged = await _merge_chunks(chunk_summaries)

    return AggregationResult(
        owner_block=merged["owner"],
        doctor_block=merged["doctor"],
        changes_block=merged["treatment"],
        total_messages=len(messages),
    )
