import time

from database import conn


def import_csv(csv_path: str, table_name: str):
    """
    Функция import_csv импортирует данные из файла .csv в таблицу при помощи SQL запроса "COPY ... FROM ..."

    :param csv_path: Путь до файла .csv
    :param table_name: Имя таблицы в базе данных
    """

    # Используем контекстный менеджер для работы с подключением и создания объекта курсора
    # Такая конструкция позволяет вызывать commit только в том случае, если во время исполнения кода
    # Не было вызвано ни одного исключения, в противном случае вызывается roll_back.
    with conn:
        with conn.cursor() as cur:
            with open(csv_path, "r", encoding='UTF8') as f:
                try:
                    st = time.time()
                    sql_copy = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER)"  # Составляем запрос для импорта
                    cur.copy_expert(sql=sql_copy, file=f)   # Вызываем метод для импорта,
                                                            # с использованием кастомного sql запроса
                    conn.commit()
                    print(f'Таблица {table_name} скопирована за {time.time() - st} сек!') # Фиксируем время выполнения

                except Exception as ex:
                    print('Функция import_csv:')
                    print(ex)


def create_tables():
    """
    Функция create_tables создаёт таблицы в базе данных, согласно данным в файлах CSV,
    при помощи sql запросов.
    """
    start_time = time.time()
    # Используем контекстный менеджер для работы с подключением и создания объекта курсора
    # Такая конструкция позволяет вызывать commit только в том случае, если во время исполнения кода
    # Не было вызвано ни одного исключения, в противном случае вызывается roll_back.
    with conn:
        with conn.cursor() as cur:
            # SQL запрос для создания таблицы cities
            sql = """
                CREATE TABLE IF NOT EXISTS cities  
                (
                city_id SERIAL PRIMARY KEY NOT NULL,
                city_ref VARCHAR UNIQUE NOT NULL,
                name VARCHAR NOT NULL
                );
                 """

            cur.execute(sql)

            # SQL запрос для создания таблицы products
            sql = """
                CREATE TABLE IF NOT EXISTS products  
                (
                prod_id SERIAL PRIMARY KEY NOT NULL,
                prod_ref VARCHAR UNIQUE NOT NULL,
                name VARCHAR NOT NULL
                );
                 """
            cur.execute(sql)

            # SQL запрос для создания таблицы branches
            sql = """
                CREATE TABLE IF NOT EXISTS branches  
                (
                branch_id SERIAL PRIMARY KEY NOT NULL,
                branch_ref VARCHAR UNIQUE NOT NULL,
                name VARCHAR NOT NULL,
                city_ref VARCHAR REFERENCES cities(city_ref),
                short_name VARCHAR,
                region VARCHAR NOT NULL
                );
                 """
            cur.execute(sql)

            # SQL запрос для создания таблицы sales
            sql = """
                CREATE TABLE IF NOT EXISTS sales  
                (
                sale_id SERIAL PRIMARY KEY NOT NULL,
                time TIMESTAMP NOT NULL,
                branch_ref VARCHAR REFERENCES branches (branch_ref),
                prod_ref VARCHAR REFERENCES products (prod_ref),
                count NUMERIC(6, 1),
                price NUMERIC(10, 2)
                );
                 """
            cur.execute(sql)

    print(f'Все таблицы созданы за {time.time() - start_time} сек')
