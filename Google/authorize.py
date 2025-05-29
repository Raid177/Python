import os
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly"
]

def main():
    if os.path.exists("token.json"):
        os.remove("token.json")
        print("🗑️ Стерто старий token.json")

    # === Працюємо відносно місця розташування скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, "credentials.json")

    flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
    creds = flow.run_local_server(port=0)

    token_path = os.path.join(script_dir, "token.json")
    with open(token_path, "w") as token:

        token.write(creds.to_json())

    print("✅ Авторизація завершена.")
    print("📄 Отримані scopes:", creds.scopes)

if __name__ == "__main__":
    main()
