import time
from tables import create_tables, import_csv
from analytics import do_analytics
from calculations import do_calculations

"""
Исполнение программы начинается при запуски файла main.py, данные переносятся из csv файлов
в базу данных postgresql, название бд находится в файле database.py, в переменной db_name
"""

start_time = time.time()

# Создам таблицы в базе данных при помощи функции create_tables() из файла tables.py
create_tables()

# Производим импорт данных из файлов .csv в базу данных
# Функция import_csv описана в файле tables.py
import_csv(csv_path='src/t_cities.csv', table_name='cities')
import_csv(csv_path='src/t_products.csv', table_name='products')
import_csv(csv_path='src/t_branches.csv', table_name='branches')
import_csv(csv_path='src/t_sales.csv', table_name='sales')

# Производим аналитику, согласно заданию, при помощи функции do_analytics() из файла analytics.py
do_analytics()

# Производим расчеты, согласно заданию, при помощи функции do_calculations() из файла calculations.py
do_calculations()

print('Время выполнения:', time.time() - start_time)
