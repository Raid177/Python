# scripts/check_openai.py
import asyncio
import json
import httpx

from core.config import settings


async def main():
    print("ENV:", settings.env)
    print("API_BASE:", settings.OPENAI_API_BASE)
    print("MODEL:", settings.OPENAI_VISIT_MODEL)

    # Щоб переконатись, що ключ з .env читається повністю
    key = settings.OPENAI_API_KEY
    print("KEY prefix:", key[:10], "... len:", len(key))

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    if settings.OPENAI_ORG_ID:
        headers["OpenAI-Organization"] = settings.OPENAI_ORG_ID
    if settings.OPENAI_PROJECT:
        headers["OpenAI-Project"] = settings.OPENAI_PROJECT

    body = {
        "model": settings.OPENAI_VISIT_MODEL,
        "messages": [
            {"role": "system", "content": "You are a test assistant."},
            {"role": "user", "content": "Скажи одним словом: ОК"},
        ],
        "temperature": 0.0,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{settings.OPENAI_API_BASE}/chat/completions",
            headers=headers,
            json=body,
        )

    print("STATUS:", resp.status_code)
    print("RAW BODY:")
    print(resp.text)


if __name__ == "__main__":
    asyncio.run(main())
