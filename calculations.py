from database import conn, engine
import pandas as pd

import time


def do_calculations():
    """
    Функция do_calculations производит расчеты, согласно заданию, записывает в файл t_classes.csv результаты разделения
    товаров на классы, а так же сохраняет их в таблицу classes в базе данных.
    """

    start_time = time.time()

    # Используем контекстный менеджер для создания объекта курсора
    with conn.cursor() as cur:
        sql = """
            select prod_ref, count(*) from sales
            group by prod_ref
            order by count desc
            """
        cur.execute(sql)
        result = cur.fetchall()

        df = pd.DataFrame(result, columns=['Номенклатура', 'Количество продаж']) # Создаем датафрейм из резульата sql запроса
        top_quantile = df['Количество продаж'].quantile(0.9)    # Находим верхнюю границу продаж
        bottom_quantile = df['Количество продаж'].quantile(0.3) # Находим нижнюю границу продаж

        # Функция, возвращающая класс товара, исходя из его продаж, по известным границам
        def check_class(x):
            if x > top_quantile:
                return 'Самый продаваемый'
            elif x > bottom_quantile:
                return 'Средне продаваемый'
            return 'Наименее продаваемый'

        df['Класс'] = df['Количество продаж'].apply(check_class)    # Применяем функцию ко всему столбцу 'Количество продаж'
        df = df.drop(columns=['Количество продаж'], axis=1)    # Удаляем столбец 'Количество продаж'
        df.to_csv(r'src/t_classes.csv', index=False)    # Сохраняем датафрейм в файл .csv
        df = df.rename(columns={'Номенклатура': 'prod_ref', 'Класс': 'class'})   # Переименовываем столбцы для сохранения в бд
        df.to_sql('classes', con=engine, if_exists='replace', index=False)  # Сохраняем датафрейм в таблицу бд classes

        print(f'Функция do_calculations завершила свою работу за {time.time() - start_time}')
