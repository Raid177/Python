from googleapiclient.discovery import build

# Ідентифікатори
spreadsheet_id = sheet.spreadsheet.id
sheet_id = sheet.id

# A1 -> GridRange
def a1_to_gridrange(a1):
    from gspread.utils import a1_to_rowcol
    col, row = a1_to_rowcol(a1)
    return {
        "sheetId": sheet_id,
        "startRowIndex": row - 1,
        "endRowIndex": row,
        "startColumnIndex": col - 1,
        "endColumnIndex": col
    }

# Формат запиту на перекреслення
requests = []
for cell in ["A1", "B2", "C3", "C10"]:
    requests.append({
        "repeatCell": {
            "range": a1_to_gridrange(cell),
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "strikethrough": True
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat.strikethrough"
        }
    })

# Надсилання
service = build("sheets", "v4", credentials=creds)
service.spreadsheets().batchUpdate(
    spreadsheetId=spreadsheet_id,
    body={"requests": requests}
).execute()

print("✅ Перекреслення встановлено напряму через API")
