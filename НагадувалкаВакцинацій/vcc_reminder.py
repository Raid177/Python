#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Формуємо таблицю нагадувань про сказ (rabies) по останній вакцинації за тварину
з урахуванням «побачених, але не відібраних» дат.

Джерела (MySQL):
- et_InformationRegister_ПрофилактическиеРаботы_RecordType (Active=1, ПрофилактическаяРабота_Key, Period >= CURDATE() - INTERVAL 12 MONTH)
- et_Catalog_Карточки (кличка, № договору, Хозяин_Key)
- et_Catalog_Породы (вид)

API Enote: hs/api/v2/GetClient (по Хозяин_Key) → owner_first/last/middle/phone.

Особливості:
- Дедуплікація рядків у межах ОДНІЄЇ вакцинації: один запис на (Карточка_Key, Period).
- Якщо прод API повертає null, автоматично пробуємо BASE+'-copy'.
- Телефон зберігаємо як ТІЛЬКИ ЦИФРИ (без +), нормалізуємо під формат 380XXXXXXXXX.

Вивід:
- upsert у `vcc_reminder`
- консоль: записи з next_due_date ∈ [сьогодні; +30 днів]

Залежності:
  pip install mysql-connector-python python-dotenv requests tabulate
"""

from __future__ import annotations
import os
import sys
import json
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Tuple

import requests
from mysql.connector import connect
from dotenv import load_dotenv
from tabulate import tabulate

# ===== Константи =====
RABIES_WORK_KEY = "9c259f0c-4dfe-11ef-978c-2ae983d8a0f0"  # ключ сказу
TABLE_NAME = "vcc_reminder"
WINDOW_MONTHS = 12
REMINDER_WINDOW_DAYS = 30

# ===== SQL: беремо по ОДНІЙ строчці на (Карточка_Key, Period) =====
SQL_ALL = """
SELECT
  t.card_ref_key,
  t.vacc_date,
  t.pet_name,
  t.species,
  t.contract_number,
  t.owner_ref_key
FROM (
  SELECT
    ir.`Карточка_Key`                           AS card_ref_key,
    ir.`Period`                                 AS vacc_date,
    c.`Description`                             AS pet_name,
    b.`Description`                             AS species,
    c.`НомерДоговора`                           AS contract_number,
    c.`Хозяин_Key`                              AS owner_ref_key,
    ROW_NUMBER() OVER (
      PARTITION BY ir.`Карточка_Key`, ir.`Period`
      ORDER BY ir.`Period` DESC
    ) AS rn
  FROM `et_InformationRegister_ПрофилактическиеРаботы_RecordType` ir
  JOIN `et_Catalog_Карточки` c
    ON c.`Ref_Key` = ir.`Карточка_Key`
  LEFT JOIN `et_Catalog_Породы` b
    ON b.`Ref_Key` = c.`Вид_Key`
  WHERE ir.`Active` = 1
    AND ir.`ПрофилактическаяРабота_Key` = %s
    AND ir.`Period` >= (CURDATE() - INTERVAL %s MONTH)
) AS t
WHERE t.rn = 1
ORDER BY t.card_ref_key, t.vacc_date DESC;
"""

INS_UPS = f"""
INSERT INTO `{TABLE_NAME}` (
  `card_ref_key`, `pet_name`, `species`, `contract_number`,
  `owner_ref_key`, `owner_name`, `owner_phone`,
  `owner_first_name`, `owner_last_name`, `owner_middle_name`,
  `last_vacc_date`, `next_due_date`, `source_rowcount`,
  `ignored_count`, `ignored_dates`
) VALUES (
  %(card_ref_key)s, %(pet_name)s, %(species)s, %(contract_number)s,
  %(owner_ref_key)s, %(owner_name)s, %(owner_phone)s,
  %(owner_first_name)s, %(owner_last_name)s, %(owner_middle_name)s,
  %(last_vacc_date)s, %(next_due_date)s, %(source_rowcount)s,
  %(ignored_count)s, %(ignored_dates)s
) ON DUPLICATE KEY UPDATE
  `pet_name` = VALUES(`pet_name`),
  `species` = VALUES(`species`),
  `contract_number` = VALUES(`contract_number`),
  `owner_ref_key` = VALUES(`owner_ref_key`),
  `owner_name` = VALUES(`owner_name`),
  `owner_phone` = VALUES(`owner_phone`),
  `owner_first_name` = VALUES(`owner_first_name`),
  `owner_last_name` = VALUES(`owner_last_name`),
  `owner_middle_name` = VALUES(`owner_middle_name`),
  `last_vacc_date` = VALUES(`last_vacc_date`),
  `next_due_date` = VALUES(`next_due_date`),
  `source_rowcount` = VALUES(`source_rowcount`),
  `ignored_count` = VALUES(`ignored_count`),
  `ignored_dates` = VALUES(`ignored_dates`);
"""

# ===== ENV / DB =====
def load_env() -> Dict[str, Any]:
    load_dotenv()
    cfg = {
        "DB_HOST": os.getenv("DB_HOST", "127.0.0.1"),
        "DB_PORT": int(os.getenv("DB_PORT", "3306")),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_DATABASE": os.getenv("DB_DATABASE"),
        "ENOTE_API_BASE": (os.getenv("ENOTE_API_BASE") or "").rstrip("/"),
        "ENOTE_APIKEY": os.getenv("ENOTE_APIKEY"),
    }
    miss = [k for k, v in cfg.items() if not v]
    if miss:
        print(f"[WARN] В .env відсутні або порожні: {', '.join(miss)}")
    return cfg

def get_conn(cfg):
    return connect(
        host=cfg["DB_HOST"],
        port=cfg["DB_PORT"],
        user=cfg["DB_USER"],
        password=cfg["DB_PASSWORD"],
        database=cfg["DB_DATABASE"],
        autocommit=False,
        charset="utf8mb4",
        collation="utf8mb4_0900_ai_ci",
    )

# ===== API helpers =====
_owner_cache: Dict[str, Tuple[str, str, str, str, str]] = {}  # full_name, phone_digits, fn, ln, mn

def normalize_phone_digits(raw: str) -> str:
    """
    Повертаємо лише цифри (без '+'). Нормалізуємо під формат 380XXXXXXXXX.
    """
    import re
    digits = re.sub(r"\D", "", str(raw or ""))
    if not digits:
        return ""
    if digits.startswith("380") and len(digits) == 12:
        return digits
    if digits.startswith("0") and len(digits) == 10:
        return "38" + digits  # -> 380XXXXXXXXX
    if digits.startswith("80") and len(digits) == 11:
        return "3" + digits   # іноді так трапляється -> 380XXXXXXXXX
    # Якщо приходить інший варіант (напр. 63xxxxxxx без 0) — як є
    return digits

def api_get_owner(cfg, owner_ref_key: str) -> Tuple[str, str, str, str, str]:
    """
    1) Спершу пробуємо PROD:
       - ?id=<Ref_Key> (новий формат відповіді)
       - ?Ref_Key=<Ref_Key> (деякі інсталяції)
       Якщо прийшов JSON-об'єкт — парсимо і повертаємо.
    2) Якщо на PROD порожньо/None — фолбек на BASE+'-copy' з ?id=<Ref_Key>.
    Повертаємо (full_name, phone_digits, firstName, lastName, middleName)
    """
    if not owner_ref_key:
        return "", "", "", "", ""
    if owner_ref_key in _owner_cache:
        return _owner_cache[owner_ref_key]

    base = cfg.get("ENOTE_API_BASE") or ""
    apikey = cfg.get("ENOTE_APIKEY")
    if not base or not apikey:
        _owner_cache[owner_ref_key] = ("", "", "", "", "")
        return "", "", "", "", ""

    def _call(base_url: str, param_name: str):
        url = f"{base_url}/hs/api/v2/GetClient"
        try:
            r = requests.get(
                url,
                headers={"apikey": apikey, "Accept": "application/json"},
                params={param_name: owner_ref_key},
                timeout=15,
            )
        except Exception:
            return None
        if not r.ok:
            return None
        try:
            return r.json()
        except Exception:
            return None

    # PROD
    data = _call(base, "id")
    if not isinstance(data, dict) or not data:
        data = _call(base, "Ref_Key")

    # COPY fallback
    if not isinstance(data, dict) or not data:
        base_copy = base + "-copy"
        data = _call(base_copy, "id")
        if isinstance(data, dict) and data:
            print(f"[WARN] Prod GetClient returned null for {owner_ref_key}; -copy returned data. Using copy.")
        else:
            _owner_cache[owner_ref_key] = ("", "", "", "", "")
            return "", "", "", "", ""

    # Ім'я
    fn = (data.get("firstName") or "").strip()
    ln = (data.get("lastName") or "").strip()
    mn = (data.get("middleName") or "").strip()

    if not (fn or ln or mn):
        desc = (data.get("Description") or data.get("Name") or "").strip()
        if desc:
            parts = [p for p in desc.split() if p]
            if len(parts) == 1:
                ln = parts[0]
            elif len(parts) == 2:
                ln, fn = parts
            else:
                ln, fn, mn = parts[0], parts[1], " ".join(parts[2:])

    full_name = " ".join([x for x in (ln, fn, mn) if x])

    # Телефон → тільки цифри
    phone_digits = ""
    ci = data.get("contact_information")
    if isinstance(ci, list):
        for entry in ci:
            t = (entry.get("type") or "").upper()
            v = (entry.get("value") or "").strip()
            if t == "PHONE_NUMBER" and v:
                phone_digits = normalize_phone_digits(v)
                if phone_digits:
                    break
    if not phone_digits:
        legacy = (
            data.get("Phone")
            or data.get("Телефон")
            or data.get("МобильныйТелефон")
            or ""
        )
        phone_digits = normalize_phone_digits(legacy)
    if not phone_digits and isinstance(data.get("Phones"), list):
        for p in data["Phones"]:
            cand = p.get("Value") or p.get("Номер") or p.get("Number") or ""
            phone_digits = normalize_phone_digits(cand)
            if phone_digits:
                break

    _owner_cache[owner_ref_key] = (full_name, phone_digits, fn, ln, mn)
    return full_name, phone_digits, fn, ln, mn

# ===== Data fetch / group =====
def fetch_all_rows(cur) -> List[Dict[str, Any]]:
    cur.execute(SQL_ALL, (RABIES_WORK_KEY, WINDOW_MONTHS))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def group_latest_with_ignored(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    В rows уже немає дублів на рівні (Карточка_Key, Period), але
    додатково страхуємось: агрегуємо унікальні datetime.
    """
    out: List[Dict[str, Any]] = []
    i = 0
    n = len(rows)
    while i < n:
        card = rows[i]["card_ref_key"]
        group: List[Dict[str, Any]] = []
        while i < n and rows[i]["card_ref_key"] == card:
            group.append(rows[i])
            i += 1

        # Унікальні datetime у спадному порядку (як у вихідному сортуванні)
        unique_dt: List[datetime] = []
        seen = set()
        for g in group:
            vd = g["vacc_date"]
            if isinstance(vd, date) and not isinstance(vd, datetime):
                vd = datetime(vd.year, vd.month, vd.day)
            if vd not in seen:
                seen.add(vd)
                unique_dt.append(vd)

        latest_dt = unique_dt[0]
        ignored = unique_dt[1:]

        head = group[0]
        out.append({
            "card_ref_key": head["card_ref_key"],
            "last_vacc_date": latest_dt,
            "pet_name": head.get("pet_name") or "",
            "species": head.get("species") or "",
            "contract_number": head.get("contract_number") or "",
            "owner_ref_key": head.get("owner_ref_key"),
            "ignored_count": len(ignored),
            "ignored_dates": [d.strftime("%Y-%m-%d") for d in ignored],
            "source_rowcount": len(group),  # скільки сирих рядків бачили (для аудиту)
        })
    return out

# ===== Upsert / Print =====
def upsert_summary(cur, groups: List[Dict[str, Any]], cfg) -> List[Dict[str, Any]]:
    batch = []
    for g in groups:
        owner_name, owner_phone_digits, fn, ln, mn = api_get_owner(cfg, g.get("owner_ref_key"))
        next_due = g["last_vacc_date"] + timedelta(days=365)
        payload = {
            "card_ref_key": g["card_ref_key"],
            "pet_name": g.get("pet_name", ""),
            "species": g.get("species", ""),
            "contract_number": g.get("contract_number", ""),
            "owner_ref_key": g.get("owner_ref_key"),
            "owner_name": owner_name,
            "owner_phone": owner_phone_digits,  # збережено ТІЛЬКИ ЦИФРИ
            "owner_first_name": fn,
            "owner_last_name": ln,
            "owner_middle_name": mn,
            "last_vacc_date": g["last_vacc_date"],
            "next_due_date": next_due,
            "source_rowcount": g.get("source_rowcount", 1),
            "ignored_count": g.get("ignored_count", 0),
            "ignored_dates": json.dumps(g.get("ignored_dates", []), ensure_ascii=False),
        }
        batch.append(payload)
    if batch:
        cur.executemany(INS_UPS, batch)
    return batch

def print_console_reminders(batch: List[Dict[str, Any]]):
    today = date.today()
    until = today + timedelta(days=REMINDER_WINDOW_DAYS)
    rows = []
    for r in batch:
        due_dt = r["next_due_date"]
        due = due_dt.date() if isinstance(due_dt, datetime) else due_dt
        if today <= due <= until:
            last_dt = r["last_vacc_date"]
            last = last_dt.strftime("%Y-%m-%d") if isinstance(last_dt, (datetime, date)) else str(last_dt)
            rows.append([
                last,
                r.get("pet_name", ""),
                r.get("species", ""),
                r.get("contract_number", ""),
                r.get("owner_name", ""),
                r.get("owner_phone", ""),  # уже цифри
                due.strftime("%Y-%m-%d"),
                r.get("ignored_count", 0),
            ])
    if rows:
        print(tabulate(rows, headers=[
            "Дата вакцинації",
            "Кличка",
            "Вид",
            "№ договору",
            "Власник (ПІБ)",
            "Телефон (digits)",
            "Ревакц. до",
            "Ігноровано (шт)",
        ], tablefmt="github"))
    else:
        print("[INFO] У найближчі 30 днів ревакцинацій за умовами відбору не знайдено.")

# ===== main =====
def main():
    cfg = load_env()
    with get_conn(cfg) as conn:
        cur = conn.cursor()
        all_rows = fetch_all_rows(cur)
        print(f"[INFO] Знайдено вакцинацій у вікні {WINDOW_MONTHS} міс: {len(all_rows)}")
        grouped = group_latest_with_ignored(all_rows)
        print(f"[INFO] Карток (унікальних тварин) з останньою вакцинацією: {len(grouped)}")
        batch = upsert_summary(cur, grouped, cfg)
        conn.commit()
        print_console_reminders(batch)
        print(f"[INFO] Записано/оновлено у {TABLE_NAME}: {len(batch)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(130)
