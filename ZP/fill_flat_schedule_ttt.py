#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# timesheet_flatten_sa.py
# 1) Читає "Графік" → 2) формує "фкт_ГрафікПлаский" (зберігає Факт/Коментар)
# 3) Перевіряє перехльости (в т.ч. через північ, лише між сусідніми змінами)
# 4) Пише помилки в колонку Error і перекреслює рядки (без колонки Error)

import os
import time
import random
from datetime import datetime, timedelta

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# === Налаштування ===
SPREADSHEET_NAME = "zp_PetWealth"
SOURCE_SHEET = "Графік"
TARGET_SHEET = "фкт_ГрафікПлаский"

# 🔑 шлях до сервісного ключа
SA_JSON_PATH = "/root/Python/_Acces/zppetwealth-770254b6d8c1.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]

RETRY_STATUS = (500, 503)

HEADER_ROW = [
    "Дата зміни", "IDX", "ПочатокЗміни", "КінецьЗміни", "Посада",
    "Відділення", "ТипЗміни", "Прізвище",
    "ФактПочаток", "ФактКінець", "Коментар", "Error"
]


def get_worksheet_with_retry(spreadsheet, title, create_if_missing=False, rows=100, cols=12, retries=6):
    """Повертає worksheet з ретраями для 5xx і опціональним створенням при 404."""
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            return spreadsheet.worksheet(title)
        except APIError as e:
            status = None
            try:
                status = getattr(e, "response", None) and e.response.status_code
            except Exception:
                pass

            msg = str(e)

            # 404 — аркуша нема
            if (status == 404) or ("NOT_FOUND" in msg and "Requested entity" in msg):
                if create_if_missing:
                    return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
                raise

            # 5xx або backendError — ретрай з бек-офом
            if (status in RETRY_STATUS) or ("Internal error encountered" in msg) or ("backendError" in msg):
                if attempt == retries:
                    raise
                time.sleep(delay + random.uniform(0, 0.5))
                delay *= 2
                continue

            # Інші помилки — проброс
            raise


def main():
    # === Авторизація Google (service account) ===
    if not os.path.exists(SA_JSON_PATH):
        raise SystemExit(f"[ERROR] SA_JSON_PATH not found: {SA_JSON_PATH}")
    creds = Credentials.from_service_account_file(SA_JSON_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

    # Відкриваємо таблицю по назві
    spreadsheet = client.open(SPREADSHEET_NAME)
    src_ws = get_worksheet_with_retry(spreadsheet, SOURCE_SHEET, create_if_missing=False)
    tgt_ws = get_worksheet_with_retry(spreadsheet, TARGET_SHEET, create_if_missing=True, rows=100, cols=len(HEADER_ROW))

    spreadsheet_id = spreadsheet.id

    # sheetId для цільового листа
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for s in meta.get('sheets', []):
        if s.get('properties', {}).get('title') == TARGET_SHEET:
            sheet_id = s['properties']['sheetId']
            break
    if sheet_id is None:
        raise RuntimeError(f"Sheet '{TARGET_SHEET}' not found (even after creation).")

    print("[INFO] Авторизація пройдена. Завантаження існуючих даних...")

    # === Завантаження існуючих даних із пласкої таблиці ===
    existing_data = tgt_ws.get_all_values() or []
    if not existing_data:
        existing_data = [HEADER_ROW]

    # Збереження користувацьких полів (Факт/Коментар) з попередньої версії
    user_fields = {}
    for row in existing_data[1:]:
        if len(row) >= 12:
            key = tuple(row[:8])  # перші 8 колонок
            user_fields[key] = (row[8], row[9], row[10])

    # === Очистка пласкої таблиці (залишаємо тільки хедер) ===
    tgt_ws.clear()
    tgt_ws.append_row(HEADER_ROW, value_input_option="USER_ENTERED")
    print("[OK] Таблиця очищена та заголовок додано.")

    # === Завантаження даних із таблиці "Графік" ===
    data = src_ws.get_all_values()
    if not data:
        print("[WARN] Лист 'Графік' порожній.")
        return
    header = data[0]
    flat_data = []

    print("[INFO] Формування нових рядків...")
    for row in data[1:]:
        # Очікується, що в першій колонці рядка "Графіка" зберігається "MM.YYYY"
        base = row[:7]
        try:
            month_year_str = (base[0] or "").strip()
            month, year = map(int, month_year_str.split("."))
        except Exception:
            # пропускаємо технічні/порожні блоки
            continue

        for col in range(7, len(row)):
            cell_value = (row[col] or "").strip()
            # у хедері днів мають бути числа 1..31
            try:
                day = int(header[col])
            except Exception:
                continue

            try:
                date_str = datetime(year, month, day).strftime("%d.%m.%Y")
            except Exception:
                continue

            if not cell_value:
                continue

            # Порядок має відповідати першим 8 колонкам HEADER_ROW:
            # [Дата, IDX, Початок, Кінець, Посада, Відділення, ТипЗміни, Прізвище]
            # base: [0]=MM.YYYY, [1]=Посада, [2]=Відділення, [3]=ТипЗміни, [4]=ПочатокЗміни, [5]=КінецьЗміни, [6]=IDX
            key = (date_str, base[6], base[4], base[5], base[1], base[2], base[3], cell_value)
            fact_start, fact_end, comment = user_fields.get(key, ("", "", ""))

            flat_data.append([
                date_str, base[6], base[4], base[5], base[1], base[2], base[3], cell_value,
                fact_start, fact_end, comment, ""
            ])

    if flat_data:
        tgt_ws.append_rows(flat_data, value_input_option="USER_ENTERED")
        print(f"[OK] Додано {len(flat_data)} актуальних рядків.")
    else:
        print("[WARN] Даних для вставки немає у таблиці 'Графік'.")

    # === Перевірка на конфлікти ===
    print("[INFO] Перевірка на конфлікти...")
    existing_data = tgt_ws.get_all_values() or [HEADER_ROW]
    existing_data_rows = existing_data[1:]
    error_rows = set()
    flat_data_grouped = {}

    # Групуємо зміни по прізвищу
    for row in existing_data_rows:
        if len(row) >= 12:
            date, idx, start, end, posada, viddil, shift_type, surname = row[:8]
            # Факт мають пріоритет
            fact_start = row[8].strip() if (len(row) > 8 and row[8].strip()) else start
            fact_end = row[9].strip() if (len(row) > 9 and row[9].strip()) else end

            try:
                dt_start = datetime.strptime(date + " " + fact_start, "%d.%m.%Y %H:%M")
                dt_end = datetime.strptime(date + " " + fact_end, "%d.%m.%Y %H:%M")
                if dt_end <= dt_start:
                    dt_end += timedelta(days=1)  # перехід через північ
            except Exception:
                # пропускаємо рядки з некоректними часами
                continue

            flat_data_grouped.setdefault(surname, []).append((dt_start, dt_end, idx, posada, viddil, row))

    conflicts = []
    for surname, shifts in flat_data_grouped.items():
        sorted_shifts = sorted(shifts, key=lambda x: x[0])
        for i in range(len(sorted_shifts) - 1):
            start1, end1, idx1, pos1, vid1, row1 = sorted_shifts[i]
            start2, end2, idx2, pos2, vid2, row2 = sorted_shifts[i + 1]

            if start2 < end1:
                date1 = start1.strftime("%d.%m.%Y")
                date2 = start2.strftime("%d.%m.%Y")
                s1 = start1.strftime("%H:%M")
                e1 = end1.strftime("%H:%M")
                s2 = start2.strftime("%H:%M")
                e2 = end2.strftime("%H:%M")

                conflict_text = (
                    f"[WARN] Конфлікт: {surname} між {pos1} ({s1}-{e1}, {date1}) та "
                    f"{pos2} ({s2}-{e2}, {date2})"
                )
                conflicts.append(conflict_text)

                # позиції рядків у поточному масиві (для довідки в тексті)
                try:
                    row1_index = existing_data_rows.index(row1) + 2  # +1 хедер, +1 1-based
                    row2_index = existing_data_rows.index(row2) + 2
                except ValueError:
                    row1_index = row2_index = -1

                # Запис у колонку Error (локально)
                row1[11] = f"Перетин з №{row2_index}: {date2}, {idx2}, {s2}-{e2}, {pos2}, {vid2}"
                row2[11] = f"Перетин з №{row1_index}: {date1}, {idx1}, {s1}-{e1}, {pos1}, {vid1}"

                error_rows.add(tuple(row1))
                error_rows.add(tuple(row2))

    print(f"[INFO] Виявлено {len(conflicts)} конфлікт(ів)")

    # === Оновлюємо колонку Error ШВИДШИМ СПОСОБОМ (по довжині col_values(3)) ===
    print("[INFO] Оновлюємо колонку 'Error'...")
    error_column_values = []
    for row in existing_data_rows:
        if tuple(row) in error_rows:
            error_column_values.append([row[11]])
        else:
            error_column_values.append([""])

    if error_column_values:
        # визначаємо останній заповнений рядок по колонці "ПочатокЗміни" (3-я колонка)
        start_row = 2  # з другого (після хедера)
        date_col = tgt_ws.col_values(3)  # 3 = "ПочатокЗміни"
        last_row_index = max(len(date_col), start_row - 1)  # захист від порожнього листа

        tgt_ws.update(
            f"L{start_row}:L{last_row_index}",
            error_column_values[: max(0, last_row_index - start_row + 1)],
            value_input_option="USER_ENTERED"
        )

    # === Перекреслення рядків (без колонки Error) через batchUpdate ===
    print("[INFO] Формуємо перекреслення...")
    requests = []
    for idx, row in enumerate(existing_data_rows, start=2):
        strikethrough = bool(tuple(row) in error_rows)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": idx - 1,
                    "endRowIndex": idx,
                    "startColumnIndex": 0,
                    "endColumnIndex": 11  # лише перші 11 колонок (без 'Error')
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"strikethrough": strikethrough}
                    }
                },
                "fields": "userEnteredFormat.textFormat.strikethrough"
            }
        })

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

    if conflicts:
        print("\n[WARN] Знайдені конфлікти:")
        for c in conflicts:
            print(c)
    else:
        print("\n[OK] Конфліктів не знайдено.")

    print("\n[OK] Завершено: таблицю оновлено та перевірено на перехльости.")


if __name__ == "__main__":
    main()
