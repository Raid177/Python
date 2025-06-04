# # Формуємо дані Єнота для роботи (продажі)

# import pymysql
# import pandas as pd
# from sqlalchemy import create_engine
# from dotenv import load_dotenv
# import os

# # Завантаження .env
# load_dotenv(r"C:\Users\la\OneDrive\Pet Wealth\Analytics\Python_script\.env")
# DB_HOST = os.getenv("DB_HOST")
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_DATABASE = os.getenv("DB_DATABASE")

# # Підключення
# engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}")

# # 1. Завантажити дані через SQL (те, що писали вище)
# sql_query = """
#     [Тут вставляєш свій SELECT з UNION ALL]
# """

# df = pd.read_sql(sql_query, engine)

# # 2. Очистити таблицю за період
# with engine.connect() as conn:
#     conn.execute("""
#         DELETE FROM wealth0_analytics.zp_sales_salary
#         WHERE Period >= '2025-05-01'
#     """)

# # 3. Записати нові дані
# df.to_sql('zp_sales_salary', con=engine, if_exists='append', index=False)

# print(f"✅ Дані за {len(df)} рядків успішно оновлено!")
