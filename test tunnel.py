import pymysql

try:
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="olexii_raid",
        password="Z4vBrpT7@9uLMxWg",
        database="petwealth"
    )
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES;")
        print("✅ Підключення успішне. Таблиці в БД:")
        for row in cur.fetchall():
            print("-", row[0])
except Exception as e:
    print("❌ Помилка підключення:", e)
finally:
    if 'conn' in locals():
        conn.close()
