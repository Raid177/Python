# pip install requests
import requests
import re

APIKEY   = "917881f0-62a2-4f37-a826-bf08ef581239"
BASE_URL = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262-copy"
REF_KEY  = "7ddfc99e-7744-11f0-8005-2ae983d8a0f0"

def get_client(ref_key: str) -> dict | None:
    url = f"{BASE_URL}/hs/api/v2/GetClient"
    r = requests.get(url, headers={"apikey": APIKEY}, params={"id": ref_key}, timeout=15)
    print(f"[{r.status_code}] GET {url}")
    if not r.ok:
        print("Помилка:", r.text[:1000])
        return None
    try:
        return r.json()
    except Exception:
        print("Не JSON:", r.text[:500])
        return None

def normalize_ua_phone(raw: str) -> str:
    # залишаєм лише цифри і +, забираєм пробіли/(),-
    s = re.sub(r"[^\d+]", "", str(raw))
    # варіанти нормалізації під Україну
    if s.startswith("+380") and len(s) == 13:
        return s
    if s.startswith("380") and len(s) == 12:
        return "+" + s
    if s.startswith("0") and len(s) == 10:
        return "+38" + s
    # інакше повертаємо як є
    return s

def extract_name(d: dict) -> str:
    last  = (d.get("lastName") or "").strip()
    first = (d.get("firstName") or "").strip()
    mid   = (d.get("middleName") or "").strip()
    parts = [p for p in (last, first, mid) if p]
    return " ".join(parts) or "(без імені)"

def extract_phones(d: dict) -> list[str]:
    phones = []
    for ci in (d.get("contact_information") or []):
        if (ci.get("type") or "").upper() == "PHONE_NUMBER" and ci.get("value"):
            phones.append(normalize_ua_phone(ci["value"]))
    # унікалізація в порядку появи
    seen, out = set(), []
    for p in phones:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

if __name__ == "__main__":
    data = get_client(REF_KEY)
    if not data:
        raise SystemExit(1)

    name = extract_name(data)
    phones = extract_phones(data)

    print("\n=== КЛІЄНТ ===")
    print("Ref_Key:", data.get("enoteId"))
    print("ПІБ:", name)
    print("Телефони:", phones if phones else "—")
