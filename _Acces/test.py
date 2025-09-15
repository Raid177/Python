# test_gsheets_sa_by_gid.py
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SPREADSHEET_ID = "1XGtNB6ex8v50SGX7ucoiWFNFQayBEH0g48Vz1zY9NmU"  # нова таблиця з твого URL
TARGET_GID     = 0                                                # який аркуш: ?gid=0
SA_JSON_PATH   = "/root/Python/_Acces/zppetwealth-770254b6d8c1.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_sheet_title_by_gid(service, spreadsheet_id: str, gid: int) -> str:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("sheetId") == gid:
            return props.get("title")
    raise RuntimeError(f"Аркуш із gid={gid} не знайдено")

def main():
    creds = Credentials.from_service_account_file(SA_JSON_PATH, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    sheet = service.spreadsheets()

    title = get_sheet_title_by_gid(service, SPREADSHEET_ID, TARGET_GID)
    a1 = f"'{title}'!A1"  # завжди бери в апострофи на випадок пробілів/кирилиці

    # 1) Read A1
    res = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=a1).execute()
    before = (res.get("values") or [[""]])[0][0]
    print("A1 (before):", before)

    # 2) Write "OK" to A1
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=a1,
        valueInputOption="RAW",
        body={"values": [["OK"]]},
    ).execute()
    print("A1 (after):  OK")

if __name__ == "__main__":
    main()
