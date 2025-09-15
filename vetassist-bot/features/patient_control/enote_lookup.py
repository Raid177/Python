# enote_lookup.py
# Завдання: отримати з Єнота GUID (Ref_Key) і кличку (Description) за номером карти (НомерДоговора, число)

from typing import Dict, List, Optional
import requests
from requests.auth import HTTPBasicAuth
from modules.config import settings

def _auth():
    return HTTPBasicAuth(settings.odata_user, settings.odata_password)

def _base() -> str:
    if not settings.odata_url:
        raise RuntimeError("ODATA_URL не налаштовано в .env")
    return settings.odata_url.rstrip("/")

def get_patient_by_number(number_str: str) -> Optional[dict]:
    """
    Вхід: "885" (рядок, але фільтр як ЧИСЛО)
    Вихід: {"ref_key": "...", "name": "Кличка", "owner": "Власник", "raw": {...}} або None
    """
    # фільтр БЕЗ лапок, бо поле числове
    url = (
        f"{_base()}/{settings.patient_entity}"
        f"?$format=json&$top=10"
        f"&$filter=НомерДоговора eq {int(number_str)}"
        f"&$select=Ref_Key,Description,Code"
    )
    r = requests.get(url, auth=_auth(), timeout=20)
    r.raise_for_status()
    data = r.json()
    items = data.get("value", [])
    if not items:
        return None
    item = items[0]
    return {
        "ref_key": item.get("Ref_Key"),
        "name": (item.get("Description") or "").strip(),
        "owner": (item.get("Code") or "").strip(),
        "raw": item,
    }

def get_patient_names_by_numbers(numbers: List[str]) -> Dict[str, str]:
    """
    Пакетно дістає клички за списком номерів.
    Повертає { "1472": "Барні", ... } (порожній рядок, якщо не знайдено)
    """
    out: Dict[str, str] = {}
    for n in numbers:
        try:
            info = get_patient_by_number(n)
            out[n] = info["name"] if info else ""
        except Exception:
            out[n] = ""
    return out
