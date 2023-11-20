import psycopg2
from sqlalchemy import create_engine

db_name = 'dnstest'     # Название базы данных
user = 'postgres'       # Имя пользователя postgresql
pas = 'root'            # Пароль для пользователя postgresql
host = '127.0.0.1'      # Хост сервера postgresql
port = '5432'           # Порт сервера postgresql

# Создаем подключение к серверу postgresql
conn = psycopg2.connect(
    database=db_name,
    user=user,
    password=pas,
    host=host,
    port=port
)

# Создаем объект sqlAlchemy Engine, который необходим для использования pandas.to_sql() в файле calculations.py
engine = create_engine('postgresql+psycopg2://postgres:root@127.0.0.1:5433/dnstest')
