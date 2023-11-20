import time
import pandas as pd

import matplotlib.pyplot as plt

from database import conn


def print_sql_results(res):
    """
    Функция print_sql_results выводит на экран полученные значения из sql запроса в удобном виде.
    :param res:
    """
    for row in res:
        for i in row:
            print(i, end=' ')
        print()


def do_analytics():
    """
    Функция do_analytics при помощи запросов к базе данных postgresql производит аналитику согласно заданию
    :return:
    """
    start_time = time.time()

    # при помощи конекстного менеджера создаем объект курсора.
    with conn.cursor() as cur:

        # Десять первых магазинов по количеству продаж
        storage_sql = """
                select name
                from sales s 
                join branches b on (b.branch_ref = s.branch_ref)
                    where b.name not like '%клад%'
                        group by b.branch_id
                        order by count(*) desc
                        limit 10
        """
        cur.execute(storage_sql)
        top_shops = cur.fetchall()

        print('-----------------------------------------------------------')
        print('Десять первых магазинов по количеству продаж:')
        print_sql_results(top_shops)

        # Десять первых складов по количеству продаж
        shop_sql = """
            select name
            from sales s (sale_id)
            join branches b (branch_id) on (b.branch_ref = s.branch_ref)
                where b.name like '%клад%'
                    group by b.branch_id
                    order by count(*) desc
                    limit 10
            """
        cur.execute(shop_sql)
        top_storages = cur.fetchall()
        print('-----------------------------------------------------------')
        print('Десять первых складов по количеству продаж:')
        print_sql_results(top_storages)

        print('-----------------------------------------------------------')

        # Десять самых продаваемых товаров по складам
        storage_goods_sql = """
            select p.name from sales s
            join branches b on (b.branch_ref = s.branch_ref)
            join products p on (p.prod_ref = s.prod_ref)
                where b.name like '%клад%'
                    group by p.prod_id order by count(*) desc
                    limit 10
            """
        cur.execute(storage_goods_sql)
        top_goods_storage = cur.fetchall()
        print('Десять самых продаваемых товаров по складам:')
        print_sql_results(top_goods_storage)

        print('-----------------------------------------------------------')

        # Десять самых продаваемых товаров по магазинам
        goods_sql2 = """
            select p.name from sales s
            join branches b on (b.branch_ref = s.branch_ref)
            join products p on (p.prod_ref = s.prod_ref)
                where b.name not like '%клад%'
                    group by p.prod_id order by count(*) desc
                    limit 10
            """

        cur.execute(goods_sql2)
        top_goods_shop = cur.fetchall()
        print('Десять самых продаваемых товаров по магазинам:')
        print_sql_results(top_goods_shop)

        print('-----------------------------------------------------------')

        # Десять городов, в которых больше всего продавалось товаров
        cities_sql = """
            select c.name from sales s
            join branches b on (s.branch_ref = b.branch_ref)
            join cities c on (c.city_ref = b.city_ref)
                group by c.name
                    order by count(*) desc
                    limit 10
            """
        cur.execute(cities_sql)
        top_cities = cur.fetchall()
        print('Десять городов, в которых больше всего продавалось товаров:')
        print_sql_results(top_cities)

        print('-----------------------------------------------------------')

        # День недели, в который происходит наибольшее количество продаж

        # функция to_char преобразует timestamp в день недели, дополненный до 9-ти символов пробелами
        weekdays_sql = """
            select to_char(time, 'Day') as weekday, count(*) from sales 
            group by weekday
            order by count desc
            """
        cur.execute(weekdays_sql)
        top_weekdays = cur.fetchall()

        df = pd.DataFrame(top_weekdays, columns=['День недели', 'Количество продаж'])   # Создаём датафрейм
        df['День недели'] = df['День недели'].str.strip()   # Удаляем лишние пробелы, полученные функцией to_char

        cats = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']   # Создаем категории
                                                                                                # для сортировки

        df['День недели'] = pd.Categorical(df['День недели'], categories=cats, ordered=True) # Присваиваем категории
        df = df.sort_values('День недели')  # Сортируем датафрейм по дням недели

        plt.plot(df['День недели'], df['Количество продаж'], label="Продаж за день") # Создаем график продаж за неделю
        plt.legend()    # Добавляем легенду на график
        plt.show()  # Выводим график на экран

        print('День недели, в который происходит наибольшее количество продаж:')
        print(top_weekdays[0][0])

        print('-----------------------------------------------------------')

        # Час в который происходит наибольшее количество продаж
        hours_sql = """
            select extract(hour from time) as sale_hour, count(*) from sales
            group by sale_hour
            order by count desc
            """
        cur.execute(hours_sql)
        top_hours = cur.fetchall()

        df = pd.DataFrame(top_hours, columns=['Час', 'Количество продаж'])   # Создаём датафрейм
        df = df.sort_values('Час')  # Сортируем датафрейм по часам
        plt.plot(df['Час'], df['Количество продаж'], label="Продаж за час")     # Создаем график продаж в течение дня
        plt.legend()  # Добавляем легенду на график
        plt.show()  # Выводим график на экран


        print('Час в который происходит наибольшее количество продаж:')
        print(top_hours[0][0])

        print('-----------------------------------------------------------')

    print(f'Функция do_analytics завершила свою работу за {time.time() - start_time}')
