#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#отримуємо дані власника і всіх тварин по номеру договору

import os, sys, re, json, requests
from requests.auth import HTTPBasicAuth

# ==== DEMO credentials ====
ODATA_BASE = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262/odata/standard.odata"
ODATA_USER = "odata"
ODATA_PASS = "zX8a7M36yU"
API_BASE   = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262"
APIKEY     = "917881f0-62a2-4f37-a826-bf08ef581239"

CARDS      = "Catalog_Карточки"
GETCLIENT  = f"{API_BASE}/hs/api/v2/GetClient"

HEAD_ODATA = {"Accept": "application/json"}
HEAD_API   = {"Accept": "application/json", "apikey": APIKEY}
AUTH       = HTTPBasicAuth(ODATA_USER, ODATA_PASS)

# ============== helpers =====================

def norm_phone_digits(raw: str) -> str:
    d = re.sub(r"\D", "", str(raw or ""))
    if not d: return ""
    if d.startswith("380") and len(d)==12: return d
    if d.startswith("0") and len(d)==10:   return "38"+d
    if d.startswith("80") and len(d)==11:  return "3"+d
    return d

def odata_raw_query(query: str) -> dict:
    """
    Виконує GET-запит, передаючи повністю готовий URL (щоб не кодувало кирилицю)
    """
    url = f"{ODATA_BASE}/{query}"
    r = requests.get(url, headers=HEAD_ODATA, auth=AUTH, timeout=25)
    if not r.ok:
        print(f"[HTTP {r.status_code}] {r.text[:500]}")
        r.raise_for_status()
    return r.json()

def odata_get_card_by_contract(contract: str) -> dict | None:
    """
    Повертає одну картку (dict) або None
    """
    # як число
    query_num = f"{CARDS}?$format=json&$top=1&$select=Ref_Key,Description,Хозяин_Key,НомерДоговора&$filter=НомерДоговора eq {contract}"
    data = odata_raw_query(query_num).get("value", [])
    if data:
        return data[0]
    # як рядок
    query_str = f"{CARDS}?$format=json&$top=1&$select=Ref_Key,Description,Хозяин_Key,НомерДоговора&$filter=НомерДоговора eq '{contract}'"
    data = odata_raw_query(query_str).get("value", [])
    return data[0] if data else None

def odata_get_owner_cards(owner_ref: str) -> list[dict]:
    query = f"{CARDS}?$format=json&$select=Ref_Key,Description,Хозяин_Key,НомерДоговора&$filter=Хозяин_Key eq guid'{owner_ref}'&$orderby=Description"
    data = odata_raw_query(query).get("value", [])
    return data

def api_get_client(owner_ref: str) -> dict:
    r = requests.get(GETCLIENT, headers=HEAD_API, params={"id": owner_ref}, timeout=15)
    try:
        return r.json() if r.ok else {}
    except Exception:
        return {}

def extract_owner_name(c: dict) -> str:
    ln = (c.get("lastName") or "").strip()
    fn = (c.get("firstName") or "").strip()
    mn = (c.get("middleName") or "").strip()
    if ln or fn or mn:
        return " ".join(x for x in (ln, fn, mn) if x)
    return (c.get("Description") or "—").strip()

def extract_owner_phone(c: dict) -> str:
    ci = c.get("contact_information")
    if isinstance(ci, list):
        for e in ci:
            if (e.get("type") or "").upper()=="PHONE_NUMBER":
                num = norm_phone_digits(e.get("value"))
                if num: return num
    for k in ("Phone","Телефон","МобильныйТелефон","mobile","phone"):
        if c.get(k):
            num = norm_phone_digits(c[k])
            if num: return num
    return ""

# =============== main ======================

def main():
    contract = sys.argv[1].strip() if len(sys.argv)>1 else input("Введіть номер договору: ").strip()
    if not contract:
        print("Порожній номер договору."); return

    card = odata_get_card_by_contract(contract)
    if not card:
        print(f"Не знайдено карток за НомерДоговора == {contract}")
        return

    pet_ref   = card.get("Ref_Key")
    pet_name  = card.get("Description")
    owner_ref = card.get("Хозяин_Key")
    cn        = card.get("НомерДоговора")

    owner_cards = odata_get_owner_cards(owner_ref)
    client      = api_get_client(owner_ref)
    owner_name  = extract_owner_name(client) if client else "—"
    owner_phone = extract_owner_phone(client) if client else "—"

    print(f"\nНомер договору: {contract}")
    print(f"Картка: {pet_name} | Ref_Key={pet_ref} | НомерДоговора={cn}")
    print(f"\nВласник: {owner_name}")
    print(f"owner_ref: {owner_ref}")
    print(f"Телефон: {owner_phone}")
    print("\nТварини власника:")
    if not owner_cards:
        print("  — немає записів")
    else:
        for i,p in enumerate(owner_cards,1):
            print(f"  {i}. {p.get('Description')} | Ref_Key={p.get('Ref_Key')} | договір={p.get('НомерДоговора')}")

if __name__ == "__main__":
    main()
