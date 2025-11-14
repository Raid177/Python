# enote_client.py

from __future__ import annotations
import logging, requests
from app.env_loader import load_settings
log = logging.getLogger("enote")
_session: requests.Session | None = None
_base: str | None = None
_auth: tuple[str,str] | None = None
def _ensure():
    global _session, _base, _auth
    if _session is None:
        s = load_settings()
        _session = requests.Session()
        _session.headers.update({"Content-Type":"application/json; charset=utf-8"})
        _session.auth = (s.odata_user, s.odata_password)
        _base = s.enote_base_url
        _auth = (s.odata_user, s.odata_password)
def create_cashcheck(payload: dict) -> tuple[str,str]:
    _ensure()
    r = _session.post(f"{_base}Document_ДенежныйЧек", json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    ref = data.get("Ref_Key") or data.get("d",{}).get("Ref_Key")
    num = data.get("Number")  or data.get("d",{}).get("Number") or ""
    if not ref: raise RuntimeError(f"ENOTE: no Ref_Key in response: {data}")
    return ref, num
def post_cashcheck(ref_key: str) -> None:
    _ensure()
    r = _session.post(f"{_base}Document_ДенежныйЧек(guid'{ref_key}')/Post?PostingModeOperational=false", timeout=60)
    r.raise_for_status()