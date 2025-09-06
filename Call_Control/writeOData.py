# Створюємо документ Посещение в Єноті.
# Перетягуємо туди діагнози з минулого візиту
# Заповнюємо Состав документа (запитання/відповіді анкети)

import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime

# === ENV / SESSION ===
load_dotenv("/root/Python/.env")

ODATA_URL = os.getenv("ODATA_URL_COPY").rstrip("/")
ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

session = requests.Session()
session.auth = (ODATA_USER, ODATA_PASSWORD)
session.headers.update({
    "Content-Type": "application/json; charset=utf-8",
    "Accept": "application/json"
})

# ЯКУ КАРТКУ ОБРОБЛЯЄМО
CARD_KEY = "1e4b3d6e-77a9-11f0-8005-2ae983d8a0f0"

# === HELPERS ===
def oget(url: str):
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def opatch(url: str, payload: dict, add_headers: dict | None = None):
    headers = dict(session.headers)
    if add_headers:
        headers.update(add_headers)
    r = session.patch(url, data=json.dumps(payload), headers=headers, timeout=60)
    if r.status_code >= 400:
        print("PATCH ERROR:", r.status_code)
        print("URL:", url)
        print("PAYLOAD:", json.dumps(payload, ensure_ascii=False))
        print("RESP:", r.text)
        r.raise_for_status()
    return r

# === CORE ===
def create_visit() -> tuple[str, str]:
    """Створює Document_Посещение. Повертає (Ref_Key, Number)."""
    url = f"{ODATA_URL}/Document_Посещение?$format=json"
    payload = {
        "Date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "Организация_Key": "e3e20bc4-4e84-11ef-83bb-2ae983d8a0f0",
        "ТипСобытия": "99cf6e06-5230-11ee-d889-2ae983d8a0f0",
        "ТипСобытия_Type": "StandardODATA.Catalog_ШаблоныАнкет",
        "Карточка_Key": CARD_KEY,
        "Ответственный_Key": "43996fe4-4e85-11ef-83bb-2ae983d8a0f0",
        "Подразделение_Key": "7f5078ca-4dfe-11ef-978c-2ae983d8a0f0",
        "Автор_Key": "43996fe4-4e85-11ef-83bb-2ae983d8a0f0",
        "ТипОбращення": "Повторный"  # деякі конфіги допускають "ТипОбращення"/"ТипОбращения" — залиш ключ як у вашій базі
    }
    # якщо у вас саме "ТипОбращения", поверніть назву поля:
    if "ТипОбращення" in payload:
        payload["ТипОбращения"] = payload.pop("ТипОбращення")

    resp = session.post(url, data=json.dumps(payload), timeout=60)
    print("=== Створення документа ===")
    print("HTTP статус:", resp.status_code)
    data = resp.json()
    ref_key = data.get("Ref_Key")
    number = data.get("Number")
    print("Ref_Key документа:", ref_key)
    print("Номер документа:", number)
    if not ref_key:
        raise RuntimeError(f"Не отримав Ref_Key: {data}")
    return ref_key, number

def find_last_posted_visit_ref(card_key: str) -> str | None:
    """Ref_Key останнього проведеного візиту цієї картки (або None)."""
    url = (
        f"{ODATA_URL}/Document_Посещение?$format=json"
        f"&$select=Ref_Key,Date,Number"
        f"&$filter=Карточка_Key eq guid'{card_key}' and Posted eq true"
        f"&$orderby=Date desc, Number desc&$top=1"
    )
    js = oget(url)
    items = js.get("value", [])
    return items[0]["Ref_Key"] if items else None

def get_diagnoses_for_visit(ref_key: str) -> list[dict]:
    """Рядки табличної частини 'Диагнозы' конкретного візиту."""
    url = f"{ODATA_URL}/Document_Посещение(guid'{ref_key}')/Диагнозы?$format=json"
    js = oget(url)
    return js.get("value", js if isinstance(js, list) else [])

def patch_diagnoses(new_ref: str, source_rows: list[dict]) -> int:
    """Замінює 'Диагнозы' у новому документі через PATCH на сам документ."""
    if not source_rows:
        print("У попередньому візиті діагнозів немає — пропускаю PATCH.")
        return 0

    allowed = {
        "Диагноз_Key", "РасшифровкаДиагноза", "Дифдиагноз", "НомерОбрПоДиагнозу",
        "ВидДиагноза", "СнятьДиагноз", "СостояниеДиагноза", "Результат",
        "ДатаДиагноза", "Врач_Key", "ПервыйВрач_Key", "ПричинаОтказаОтЛечения_Key"
    }

    rows = []
    for i, src in enumerate(source_rows, start=1):
        row = {k: v for k, v in src.items() if k in allowed}
        if not row.get("Диагноз_Key"):
            continue
        row["LineNumber"] = str(i)
        if "НомерОбрПоДиагнозу" in row and row["НомерОбрПоДиагнозу"] not in (None, ""):
            row["НомерОбрПоДиагнозу"] = str(row["НомерОбрПоДиагнозу"])
        rows.append(row)

    url = f"{ODATA_URL}/Document_Посещение(guid'{new_ref}')?$format=json"
    opatch(url, {"Диагнозы": rows}, add_headers={"If-Match": "*"})

    print(f"=== PATCH 'Диагнозы' ===\nВстановлено рядків: {len(rows)}")
    return len(rows)

def build_default_composition() -> list[dict]:
    """Готує 4 рядки 'Состав' як у твоєму прикладі (LineNumber рядком)."""
    return [
        {
            "LineNumber": "1",
            "Вопрос_Key": "b1a1ebb2-5230-11ee-d889-2ae983d8a0f0",
            "ЭлементарныйВопрос_Key": "f8e1be18-5207-11ee-d889-2ae983d8a0f0",
            "НомерЯчейки": "0",
            "ОткрытыйОтвет": "Загальний стан, скарги",
            "ТипОтвета": "Текст",
            # "Ответ": "",
            # "Ответ_Type": "StandardODATA.Undefined"
        },
        {
            "LineNumber": "2",
            "Вопрос_Key": "0c0499e2-07e8-11e5-80ce-00155dd6780b",
            "ЭлементарныйВопрос_Key": "eca54c87-07e7-11e5-80ce-00155dd6780b",
            "НомерЯчейки": "0",
            "ОткрытыйОтвет": "Стан зі слів власника",
            "ТипОтвета": "Текст"
        },
        {
            "LineNumber": "3",
            "Вопрос_Key": "00807d20-5231-11ee-d889-2ae983d8a0f0",
            "ЭлементарныйВопрос_Key": "fd2dab84-5230-11ee-d889-2ae983d8a0f0",
            "НомерЯчейки": "0",
            "ОткрытыйОтвет": "Доповнення схеми лікування",
            "ТипОтвета": "Текст"
        },
        {
            "LineNumber": "4",
            "Вопрос_Key": "0c0499e3-07e8-11e5-80ce-00155dd6780b",
            "ЭлементарныйВопрос_Key": "eca54c88-07e7-11e5-80ce-00155dd6780b",
            "НомерЯчейки": "0",
            "ОткрытыйОтвет": "Зміни лікування",
            "ТипОтвета": "Текст"
        }
    ]

def patch_composition(new_ref: str, rows: list[dict]) -> int:
    """Заміняє табличну частину 'Состав' через PATCH на документ."""
    if not rows:
        return 0

    allowed = {
        "LineNumber", "Вопрос_Key", "ЭлементарныйВопрос_Key",
        "НомерЯчейки", "ОткрытыйОтвет", "ТипОтвета", "Ответ", "Ответ_Type"
    }

    # нормалізуємо поля й типи
    clean_rows = []
    for r in rows:
        row = {k: v for k, v in r.items() if k in allowed}
        if "LineNumber" not in row:
            row["LineNumber"] = str(len(clean_rows) + 1)
        else:
            row["LineNumber"] = str(row["LineNumber"])
        if "НомерЯчейки" in row:
            row["НомерЯчейки"] = str(row["НомерЯчейки"])
        clean_rows.append(row)

    url = f"{ODATA_URL}/Document_Посещение(guid'{new_ref}')?$format=json"
    opatch(url, {"Состав": clean_rows}, add_headers={"If-Match": "*"})

    print(f"=== PATCH 'Состав' ===\nВстановлено рядків: {len(clean_rows)}")
    return len(clean_rows)

def post_visit(ref_key: str):
    """Проводить документ через /Post."""
    post_url = f"{ODATA_URL}/Document_Посещение(guid'{ref_key}')/Post?PostingModeOperational=false&$format=json"
    r = session.post(post_url, timeout=120)
    print("=== Проведення документа ===")
    print("HTTP статус:", r.status_code)
    if r.status_code >= 400:
        print("Помилка проведення:", r.text)
        raise SystemExit(1)

def show_status(ref_key: str):
    """Показує Number/Posted і кількість рядків у 'Диагнозы'/'Состав'."""
    doc = oget(f"{ODATA_URL}/Document_Посещение(guid'{ref_key}')?$format=json")
    print("=== Статус документа ===")
    print("Number:", doc.get("Number"))
    print("Posted:", doc.get("Posted"))

    dx = oget(f"{ODATA_URL}/Document_Посещение(guid'{ref_key}')/Диагнозы?$format=json")
    dx_rows = dx.get("value", dx if isinstance(dx, list) else [])
    print("Рядків у 'Диагнозы':", len(dx_rows))

    comp = oget(f"{ODATA_URL}/Document_Посещение(guid'{ref_key}')/Состав?$format=json")
    comp_rows = comp.get("value", comp if isinstance(comp, list) else [])
    print("Рядків у 'Состав':", len(comp_rows))

# === MAIN ===
if __name__ == "__main__":
    # 1) створення
    new_ref, new_num = create_visit()

    # 2) діагнози з останнього проведеного візиту цієї картки
    prev_ref = find_last_posted_visit_ref(CARD_KEY)
    print("Останній проведений візит цієї картки (Ref_Key):", prev_ref)

    if prev_ref:
        prev_dx = get_diagnoses_for_visit(prev_ref)
        print("Знайдено діагнозів у попередньому візиті:", len(prev_dx))
        patch_diagnoses(new_ref, prev_dx)
    else:
        print("Проведених візитів не знайдено — пропускаю PATCH 'Диагнозы'.")

    # 3) додаємо 4 рядки 'Состав' перед проведенням
    default_comp = build_default_composition()
    patch_composition(new_ref, default_comp)

    # 4) проводимо документ
    post_visit(new_ref)

    # 5) контроль
    show_status(new_ref)
