#z_report_checkbox.py
import os
import time
import json
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

# =========================
# НАЛАШТУВАННЯ / КОНСТАНТИ
# =========================
load_dotenv()

AUTH_URL = "https://api.checkbox.ua/api/v1/cashier/signinPinCode"
Z_REPORT_URL = "https://api.checkbox.ua/api/v1/extended-reports/z"
REPORT_DOWNLOAD_URL = "https://api.checkbox.ua/api/v1/extended-reports/{report_id}/report.json"

# Читаємо налаштування пауз/ретраїв з .env (мають бути числа)
BATCH_SLEEP_SECONDS = int(os.getenv("BATCH_SLEEP_SECONDS", "30"))          # пауза після успішного періоду
RETRY_429_SECONDS = int(os.getenv("RETRY_429_SECONDS", "60"))              # пауза при 429
REPORT_READY_WAIT_SECONDS = int(os.getenv("REPORT_READY_WAIT_SECONDS", "10"))  # пауза перед завантаженням report.json

# Підключення до БД (Hetzner)
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

# Опис ФОПів: беремо з .env
CREDENTIALS = [
    {
        "prefix": "DPS_LAV",
        "license_key": os.getenv("DPS_LAV_LICENSE_KEY"),
        "pin_code": os.getenv("DPS_LAV_PIN_CODE"),
        "cash_register_id": os.getenv("DPS_LAV_CASH_REGISTER_ID"),
        "branch_id": os.getenv("DPS_LAV_BRANCH_ID"),
        "fop": "Льодін Олексій Володимирович",
    },
    {
        "prefix": "DPS_ZVO",
        "license_key": os.getenv("DPS_ZVO_LICENSE_KEY"),
        "pin_code": os.getenv("DPS_ZVO_PIN_CODE"),
        "cash_register_id": os.getenv("DPS_ZVO_CASH_REGISTER_ID"),
        "branch_id": os.getenv("DPS_ZVO_BRANCH_ID"),
        "fop": "Жиліна Валерія Олександрівна",
    },
    {
        "prefix": "DPS_PMA",
        "license_key": os.getenv("DPS_PMA_LICENSE_KEY"),
        "pin_code": os.getenv("DPS_PMA_PIN_CODE"),
        "cash_register_id": os.getenv("DPS_PMA_CASH_REGISTER_ID"),
        "branch_id": os.getenv("DPS_PMA_BRANCH_ID"),
        "fop": "Прийма Микола Андрійович",
    },
]

# =========================
# SQL
# =========================
INSERT_SQL = """
INSERT INTO Z_CheckBox (
    cash_register_id, branch_id, address, open_date, close_date, receipts_count,
    fiscal_number, close_fn, shift_serial, turnover, last_receipt, sell_receipts_count,
    return_receipts_count, vat_amount, tax_rates, tax_names, tax_amount_sale, tax_turnover_sale,
    tax_amount_return, tax_turnover_return, payment_names, payment_sells, payment_returns,
    initial_balance, final_balance, service_in, service_out, card_sales, card_returns,
    cash_sales, cash_returns
) VALUES (
    %(cash_register_id)s, %(branch_id)s, %(address)s, %(open_date)s, %(close_date)s, %(receipts_count)s,
    %(fiscal_number)s, %(close_fn)s, %(shift_serial)s, %(turnover)s, %(last_receipt)s, %(sell_receipts_count)s,
    %(return_receipts_count)s, %(vat_amount)s, %(tax_rates)s, %(tax_names)s, %(tax_amount_sale)s, %(tax_turnover_sale)s,
    %(tax_amount_return)s, %(tax_turnover_return)s, %(payment_names)s, %(payment_sells)s, %(payment_returns)s,
    %(initial_balance)s, %(final_balance)s, %(service_in)s, %(service_out)s, %(card_sales)s, %(card_returns)s,
    %(cash_sales)s, %(cash_returns)s
)
ON DUPLICATE KEY UPDATE
    updated_at = CURRENT_TIMESTAMP,
    turnover = VALUES(turnover),
    receipts_count = VALUES(receipts_count),
    sell_receipts_count = VALUES(sell_receipts_count),
    return_receipts_count = VALUES(return_receipts_count),
    vat_amount = VALUES(vat_amount),
    initial_balance = VALUES(initial_balance),
    final_balance = VALUES(final_balance),
    card_sales = VALUES(card_sales),
    card_returns = VALUES(card_returns),
    cash_sales = VALUES(cash_sales),
    cash_returns = VALUES(cash_returns),
    service_in = VALUES(service_in),
    service_out = VALUES(service_out)
"""

GET_LAST_OPEN_DATE_SQL = """
SELECT MAX(open_date) FROM Z_CheckBox
WHERE cash_register_id = %s AND branch_id = %s
"""

# =========================
# ДОПОМІЖНІ ФУНКЦІЇ
# =========================
def db_connect():
    return mysql.connector.connect(**DB_CONFIG)

def get_last_open_date(conn, cash_register_id: str, branch_id: str) -> datetime:
    """
    Повертає MAX(open_date) - 5 днів; якщо немає записів — 2025-01-01.
    """
    try:
        cur = conn.cursor()
        cur.execute(GET_LAST_OPEN_DATE_SQL, (cash_register_id, branch_id))
        row = cur.fetchone()
        cur.close()

        if row and row[0]:
            return row[0] - timedelta(days=5)
        else:
            return datetime(2025, 1, 1)
    except Exception as e:
        print(f"[WARN] Не вдалося отримати останню дату з БД: {e}. Використовую 2025-01-01.")
        return datetime(2025, 1, 1)

def month_periods(start_dt: datetime, end_dt: datetime):
    """
    Розбиває діапазон на помісячні інтервали (включно), обрізаючи по end_dt.
    """
    periods = []
    cur = start_dt
    while cur <= end_dt:
        # кінець місяця
        month_end = (cur.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        if month_end > end_dt:
            month_end = end_dt
        periods.append((cur, month_end))
        cur = month_end + timedelta(days=1)
    return periods

def signin(license_key: str, pin_code: str) -> str:
    headers = {
        "accept": "application/json",
        "X-Client-Name": "PetWealth-Integration",
        "X-Client-Version": "1.0",
        "X-License-Key": license_key,
        "Content-Type": "application/json",
    }
    r = requests.post(AUTH_URL, headers=headers, json={"pin_code": pin_code})
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Авторизація не вдалася: {r.status_code} {r.text}")
    return r.json().get("access_token")

def request_z_report(headers: dict, from_iso: str, to_iso: str, cash_register_id: str, branch_id: str) -> str:
    """
    Створює extended Z-звіт і повертає report_id.
    """
    payload = {
        "from_date": from_iso,
        "to_date": to_iso,
        "cash_register_id": cash_register_id,
        "branch_id": branch_id,
        "export_extension": "JSON",
    }
    r = requests.post(Z_REPORT_URL, headers=headers, json=payload)
    if r.status_code == 429:
        print(f"[WARN] 429 Too Many Requests. Чекаю {RETRY_429_SECONDS} с і повторюю...")
        time.sleep(RETRY_429_SECONDS)
        r = requests.post(Z_REPORT_URL, headers=headers, json=payload)

    if r.status_code not in (200, 201):
        raise RuntimeError(f"Помилка створення звіту: {r.status_code} {r.text}")

    rid = r.json().get("id")
    if not rid:
        raise RuntimeError("Не отримано report_id у відповіді.")
    return rid

def download_report(headers: dict, report_id: str) -> list:
    """
    Завантажує JSON звіту (список змін). Повертає list.
    """
    time.sleep(REPORT_READY_WAIT_SECONDS)  # невелика пауза, щоб звіт зібрався
    url = REPORT_DOWNLOAD_URL.format(report_id=report_id)
    r = requests.get(url, headers=headers)
    if r.status_code == 429:
        print(f"[WARN] 429 на завантаженні report.json. Чекаю {RETRY_429_SECONDS} с і повторюю...")
        time.sleep(RETRY_429_SECONDS)
        r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Помилка отримання звіту: {r.status_code} {r.text}")

    try:
        data = r.json()
    except json.JSONDecodeError:
        raise RuntimeError("Неможливо розпарсити JSON звіту.")

    if not isinstance(data, list):
        # інколи API може повертати об’єкт — нормалізуємо
        data = [data]
    return data

def normalize_report_rows(rows: list, cash_register_id: str, branch_id: str):
    """
    Приводимо записи звіту до словника полів таблиці Z_CheckBox.
    Поля з нестандартними структурами приводимо до string/decimal за потреби.
    """
    normalized = []
    for r in rows:
        # Дати з API (ISO) -> datetime
        def parse_dt(val):
            if not val:
                return None
            # Checkbox повертає ISO із зоною, mysql-connector приймає datetime без зони -> зрізаємо зміщення
            # приклад: "2025-01-17T16:11:10+00:00"
            try:
                # залишимо тільки YYYY-MM-DDTHH:MM:SS
                return datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                return None

        # числові з полями можуть бути None
        def to_dec(val):
            if val is None:
                return None
            try:
                return float(val)
            except Exception:
                return None

        # спискові поля (податки/платежі) часто приходять як масиви — перетворимо в прості рядки JSON
        def to_json_str(val):
            if val is None:
                return None
            try:
                return json.dumps(val, ensure_ascii=False)
            except Exception:
                return str(val)

        row = {
            "cash_register_id": cash_register_id,
            "branch_id": branch_id,
            "address": (r.get("address") or "").encode("utf-8", "ignore").decode("utf-8"),
            "open_date": parse_dt(r.get("open_date")),
            "close_date": parse_dt(r.get("close_date")),
            "receipts_count": int(r.get("receipts_count") or 0),
            "fiscal_number": r.get("fiscal_number"),
            "close_fn": r.get("close_fn"),
            "shift_serial": int(r.get("shift_serial") or 0),
            "turnover": to_dec(r.get("turnover") or 0),
            "last_receipt": r.get("last_receipt"),
            "sell_receipts_count": int(r.get("sell_receipts_count") or 0),
            "return_receipts_count": int(r.get("return_receipts_count") or 0),
            "vat_amount": to_dec(r.get("vat_amount")),
            "tax_rates": to_json_str(r.get("tax_rates")),
            "tax_names": to_json_str(r.get("tax_names")),
            "tax_amount_sale": to_dec(r.get("tax_amount_sale")),
            "tax_turnover_sale": to_dec(r.get("tax_turnover_sale")),
            "tax_amount_return": to_dec(r.get("tax_amount_return")),
            "tax_turnover_return": to_dec(r.get("tax_turnover_return")),
            "payment_names": to_json_str(r.get("payment_names")),
            "payment_sells": to_json_str(r.get("payment_sells")),
            "payment_returns": to_json_str(r.get("payment_returns")),
            "initial_balance": to_dec(r.get("initial_balance")),
            "final_balance": to_dec(r.get("final_balance")),
            "service_in": to_dec(r.get("service_in")),
            "service_out": to_dec(r.get("service_out")),
            "card_sales": to_dec(r.get("card_sales")),
            "card_returns": to_dec(r.get("card_returns")),
            "cash_sales": to_dec(r.get("cash_sales")),
            "cash_returns": to_dec(r.get("cash_returns")),
        }
        # обов’язкові поля для унікального ключа — якщо їх нема, пропускаємо
        if not (row["fiscal_number"] and row["close_fn"] and row["shift_serial"] is not None):
            continue
        normalized.append(row)
    return normalized

# =========================
# ОСНОВНА ЛОГІКА
# =========================
def main():
    # Підключення до БД тримаємо один раз на весь забіг
    conn = db_connect()
    cur = conn.cursor()

    try:
        for cred in CREDENTIALS:
            # Пропускаємо, якщо не заповнені обов’язкові поля
            if not (cred["license_key"] and cred["pin_code"] and cred["cash_register_id"] and cred["branch_id"]):
                print(f"[SKIP] {cred['prefix']}: не всі змінні в .env заповнені (license/pin/cash_register_id/branch_id).")
                continue

            print(f"\n[AUTH] Авторизуюсь: {cred['fop']} ({cred['prefix']}) ...")
            try:
                token = signin(cred["license_key"], cred["pin_code"])
            except Exception as e:
                print(f"[ERROR] Авторизація провалилась для {cred['prefix']}: {e}")
                continue

            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {token}",
                "X-Client-Name": "PetWealth-Integration",
                "X-Client-Version": "1.0",
                "Content-Type": "application/json",
            }

            # Діапазон: від MAX(open_date)-5 днів до ВЧОРА
            last_dt = get_last_open_date(conn, cred["cash_register_id"], cred["branch_id"])
            start_dt = last_dt
            end_dt = datetime.now() - timedelta(days=1)

            if start_dt > end_dt:
                print(f"[INFO] {cred['prefix']}: Нових періодів немає (start={start_dt}, end={end_dt}).")
                continue

            periods = month_periods(start_dt, end_dt)
            print(f"[INFO] {cred['prefix']}: Періодів до обробки: {len(periods)}")

            for p_start, p_end in periods:
                from_iso = p_start.strftime("%Y-%m-%dT00:00:00+0300")
                to_iso = p_end.strftime("%Y-%m-%dT23:59:59+0300")
                print(f"[INFO] {cred['prefix']}: Запитую {from_iso} — {to_iso}")

                # Створення звіту з ретраєм на 429 всередині
                try:
                    report_id = request_z_report(headers, from_iso, to_iso, cred["cash_register_id"], cred["branch_id"])
                except Exception as e:
                    print(f"[ERROR] {cred['prefix']}: Не вдалося створити звіт: {e}")
                    continue

                # Завантаження report.json (з ретраєм на 429)
                try:
                    rows = download_report(headers, report_id)
                except Exception as e:
                    print(f"[ERROR] {cred['prefix']}: Не вдалося завантажити report.json: {e}")
                    continue

                if not rows:
                    print(f"[INFO] {cred['prefix']}: Звіт порожній.")
                    time.sleep(1)
                    continue

                # Нормалізація та вставка у БД
                to_insert = normalize_report_rows(rows, cred["cash_register_id"], cred["branch_id"])
                if not to_insert:
                    print(f"[INFO] {cred['prefix']}: Немає валідних рядків для вставки.")
                    time.sleep(1)
                    continue

                try:
                    for rec in to_insert:
                        cur.execute(INSERT_SQL, rec)
                    conn.commit()
                    print(f"[SUCCESS] {cred['prefix']}: Записано {len(to_insert)} рядків. Пауза {BATCH_SLEEP_SECONDS} с ...")
                    time.sleep(BATCH_SLEEP_SECONDS)  # анти-флуд
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR] {cred['prefix']}: Помилка вставки у БД: {e}")

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    print("\n[FINISH] Обробка завершена.")

if __name__ == "__main__":
    main()
