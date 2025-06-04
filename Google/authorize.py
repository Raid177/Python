import os
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly"
]

def main():
    token_path = os.path.join(os.path.dirname(__file__), "token.json")
    if os.path.exists(token_path):
        os.remove(token_path)
        print("[DELETE] Стерто старий token.json")

    creds_path = os.path.join(os.path.dirname(__file__), "credentials.json")
    flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
    
    print("[INFO] Відкриваємо браузер для авторизації...")
    print("[INFO] Якщо браузер не відкриється автоматично, запустіть цей скрипт окремо вручну.")
    
    creds = flow.run_local_server(port=0)

    with open(token_path, "w") as token_file:
        token_file.write(creds.to_json())

    print("[OK] Авторизація завершена.")
    print("[INFO] Отримані scopes:", creds.scopes)

if __name__ == "__main__":
    main()
