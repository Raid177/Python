# core/integrations/enote.py
from __future__ import annotations
import re
import requests
from typing import Any, Dict, List, Optional
from requests.auth import HTTPBasicAuth
from core.config import settings

ODATA_BASE = settings.ENOTE_ODATA_URL  # .../odata/standard.odata
API_BASE = settings.ENOTE_API_URL  # .../hs/api/v2
HEAD_ODATA = {"Accept": "application/json"}
HEAD_API = {"Accept": "application/json", "apikey": settings.ENOTE_API_KEY}
AUTH = HTTPBasicAuth(settings.ENOTE_ODATA_USER, settings.ENOTE_ODATA_PASS)

CARDS = "Catalog_Карточки"
GETCLIENT = f"{API_BASE}/GetClient"


class EnoteError(Exception):
    pass


def norm_phone_digits(raw: str) -> str:
    d = re.sub(r"\D", "", str(raw or ""))
    if not d:
        return ""
    if d.startswith("380") and len(d) == 12:
        return d
    if d.startswith("0") and len(d) == 10:
        return "38" + d
    if d.startswith("80") and len(d) == 11:
        return "3" + d
    return d


def _odata_raw_query(path_query: str) -> Dict[str, Any]:
    """
    Приймає повністю сформований path+query (щоб уникати проблем з кирилицею).
    """
    if not ODATA_BASE:
        raise EnoteError("ODATA base URL is not configured")
    url = f"{ODATA_BASE}/{path_query}"
    r = requests.get(url, headers=HEAD_ODATA, auth=AUTH, timeout=25)
    if not r.ok:
        raise EnoteError(f"ODATA HTTP {r.status_code}: {r.text[:500]}")
    return r.json()


def odata_get_card_by_contract(contract: str) -> Optional[Dict[str, Any]]:
    """
    Повертає одну картку (dict) або None.
    Пробуємо числовий і рядковий фільтри.
    """
    q1 = f"{CARDS}?$format=json&$top=1&$select=Ref_Key,Description,Хозяин_Key,НомерДоговора&$filter=НомерДоговора eq {contract}"
    data = _odata_raw_query(q1).get("value", [])
    if data:
        return data[0]
    q2 = f"{CARDS}?$format=json&$top=1&$select=Ref_Key,Description,Хозяин_Key,НомерДоговора&$filter=НомерДоговора eq '{contract}'"
    data = _odata_raw_query(q2).get("value", [])
    return data[0] if data else None


def odata_get_owner_cards(owner_ref: str) -> List[Dict[str, Any]]:
    q = f"{CARDS}?$format=json&$select=Ref_Key,Description,Хозяин_Key,НомерДоговора&$filter=Хозяин_Key eq guid'{owner_ref}'&$orderby=Description"
    return _odata_raw_query(q).get("value", [])


def api_get_client(owner_ref: str) -> Dict[str, Any]:
    if not API_BASE:
        return {}
    r = requests.get(GETCLIENT, headers=HEAD_API, params={"id": owner_ref}, timeout=15)
    try:
        return r.json() if r.ok else {}
    except Exception:
        return {}


def extract_owner_name(c: Dict[str, Any]) -> str:
    ln = (c.get("lastName") or "").strip()
    fn = (c.get("firstName") or "").strip()
    mn = (c.get("middleName") or "").strip()
    if ln or fn or mn:
        return " ".join(x for x in (ln, fn, mn) if x)
    return (c.get("Description") or "—").strip()


def extract_owner_phone(c: Dict[str, Any]) -> str:
    ci = c.get("contact_information")
    if isinstance(ci, list):
        for e in ci:
            if (e.get("type") or "").upper() == "PHONE_NUMBER":
                num = norm_phone_digits(e.get("value"))
                if num:
                    return num
    for k in ("Phone", "Телефон", "МобильныйТелефон", "mobile", "phone"):
        if c.get(k):
            num = norm_phone_digits(c[k])
            if num:
                return num
    return ""
