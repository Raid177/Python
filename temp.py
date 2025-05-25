import requests
import os
from dotenv import load_dotenv

load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")
BOT_TOKEN = os.getenv("BOT_TOKEN")

commands = [
    {"command": "pay", "description": "Надіслати файл для оплати"}
]

url = f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands"
response = requests.post(url, json={"commands": commands})

print(response.status_code, response.text)
