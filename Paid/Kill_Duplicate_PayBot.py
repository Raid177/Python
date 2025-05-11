import psutil
import time

PROCESS_NAME = "Pay_Bot.exe"

def kill_duplicates():
    found = []
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if proc.info['name'] == PROCESS_NAME:
                found.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if len(found) <= 1:
        print("✅ Лише один екземпляр працює або не знайдено.")
        return

    # Зберегти перший, закрити решту
    to_keep = found[0]
    to_kill = found[1:]

    for proc in to_kill:
        try:
            proc.kill()
            print(f"❌ Завершено: PID {proc.pid}")
        except Exception as e:
            print(f"⚠️ Не вдалося завершити PID {proc.pid}: {e}")

if __name__ == "__main__":
    print("🔍 Пошук дублікатів Pay_Bot.exe ...")
    kill_duplicates()
    time.sleep(1)
