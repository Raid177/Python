# db.py
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from app.env_loader import load_settings
_pool: MySQLConnectionPool | None = None
def get_pool() -> MySQLConnectionPool:
    global _pool
    if _pool is None:
        s = load_settings()
        _pool = MySQLConnectionPool(pool_name="bank_pool", pool_size=5,
            host=s.db_host, port=s.db_port, user=s.db_user, password=s.db_password,
            database=s.db_database, autocommit=True)
    return _pool
def get_conn():
    return get_pool().get_connection()
