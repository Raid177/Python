import mysql.connector as mysql
from core.config import settings

def get_conn():
    return mysql.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
        autocommit=True,
    )
