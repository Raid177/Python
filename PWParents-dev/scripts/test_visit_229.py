# scripts/test_visit_229.py

import asyncio
import json
from pprint import pprint

from core.repositories.cnt_chat_repo import get_ticket_messages
from bot.service.cnt_gpt_aggregator import (
    _format_chunk,
    _aggregate_chunk,
    aggregate_messages,
)


async def main():
    ticket_id = 229

    print("=== 1) Витягуємо повідомлення з БД ===")
    msgs = get_ticket_messages(ticket_id)
    print(f"Отримано {len(msgs)} повідомлень.")
    pprint(msgs)

    print("\n=== 2) Формуємо текст чанку (_format_chunk) ===")
    formatted = _format_chunk(msgs)
    print(formatted)

    print("\n=== 3) Тестуємо _aggregate_chunk (сирий виклик GPT) ===")
    chunk_resp = await _aggregate_chunk(msgs)
    print("Результат _aggregate_chunk:")
    pprint(chunk_resp)

    print("\n=== 4) Тестуємо aggregate_messages (повний цикл) ===")
    agg = await aggregate_messages(msgs)
    print("Результат aggregate_messages:")
    print(agg)


if __name__ == "__main__":
    asyncio.run(main())
