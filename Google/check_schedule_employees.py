старрьо - малювало бордюри
# import os
# import requests
# import gspread
# from dotenv import load_dotenv
# from google.oauth2.credentials import Credentials
# from google.auth.transport.requests import Request
# from gspread_formatting import *
# from datetime import datetime
# import json

# # === Налаштування ===
# load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

# # Авторизація Google Sheets через token.json
# SCOPES = [
#     'https://www.googleapis.com/auth/spreadsheets',
#     'https://www.googleapis.com/auth/drive',
#     'https://www.googleapis.com/auth/drive.file',
#     'https://www.googleapis.com/auth/drive.readonly'
# ]
# script_dir = os.path.dirname(os.path.abspath(__file__))
# token_path = os.path.join(script_dir, "token.json")
# creds = Credentials.from_authorized_user_file(token_path, SCOPES)
# if creds.expired and creds.refresh_token:
#     creds.refresh(Request())

# client = gspread.authorize(creds)

# print("\n[LOG] Початок перевірки графіка —", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# # === 1. Отримати дані з Єнота ===
# ODATA_URL = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262-copy/odata/standard.odata/Catalog_ФизическиеЛица"
# response = requests.get(
#     ODATA_URL,
#     auth=(os.getenv("ODATA_USER"), os.getenv("ODATA_PASSWORD")),
#     params={
#         "$select": "Ref_Key,Code,Description",
#         "$filter": "IsFolder eq false",
#         "$format": "json"
#     }
# )
# response.raise_for_status()
# data = response.json()['value']
# print(f"[LOG] Отримано {len(data)} співробітників з Єнота")

# # Підготовка списку
# def shorten_name(full_name):
#     parts = full_name.strip().split()
#     return f"{parts[0]} {parts[1][0]}." if len(parts) >= 2 else full_name.strip()

# staff_map = {
#     entry['Ref_Key']: {
#         'ПІБ': entry['Description'].strip(),
#         'Code': entry['Code'],
#         'Графік': shorten_name(entry['Description'])
#     } for entry in data
# }

# # === 2. Оновити дов_Співробітники ===
# staff_ws = client.open("Графік").worksheet("дов_Співробітники")
# staff_data = staff_ws.get_all_records()
# manual_links = {row['Ref_Key']: row['Графік'].strip() for row in staff_data if row['Ref_Key']}

# new_rows = []
# for ref, info in staff_map.items():
#     графік = manual_links.get(ref, info['Графік'])
#     new_rows.append([info['ПІБ'], графік, info['Code'], ref])

# staff_ws.clear()
# staff_ws.append_row(["ПІБ", "Графік", "Code", "Ref_Key", "Оновлено"])
# for row in new_rows:
#     staff_ws.append_row(row + ["=NOW()"])
# print(f"[LOG] Оновлено дов_Співробітники — {len(new_rows)} рядків")

# # === 3. Перевірка графіка ===
# schedule_ws = client.open("Графік").worksheet("графік")
# schedule_data = schedule_ws.get_all_values()
# valid_names = {row[1].strip() for row in new_rows if row[1].strip()}

# print("[LOG] Починаємо перевірку клітинок з H2 по AL...")
# invalid_count = 0
# fixed_count = 0

# for row_idx, row in enumerate(schedule_data[1:], start=2):
#     for col_idx in range(8, 39):
#         name = row[col_idx-1].strip()
#         cell = gspread.utils.rowcol_to_a1(row_idx, col_idx)
#         if not name:
#             continue

#         if name not in valid_names:
#             fmt = cellFormat(border=border('SOLID', color='red', style='SOLID', width=2))
#             format_cell_range(schedule_ws, cell, fmt)
#             print(f"[❌] {cell}: '{name}' — НЕ знайдено у довіднику")
#             invalid_count += 1
#         else:
#             clear_format(schedule_ws, cell)
#             fixed_count += 1

# print(f"[LOG] Перевірка завершена. Некоректних клітинок: {invalid_count}, очищено форматів: {fixed_count}\n")
