#  Денормализуйте таблицу так, чтобы не нужно было для каждого рекламодателя
#  постоянно подсчитывать количество кампаний и продаж


import pandas as pd
from random import randint as rd
from random import choice as ch
import string
from datetime import datetime as dt
import psycopg2
from sqlalchemy import create_engine


def filling_out_data(name_column, type_data, count_line):
    """Создает столбец с указанным именем и количеством
    строк, который будет заполен рандомно созданными
    значениями, с указанной длиной символов"""
    match type_data:
        case "object":
            chars = string.ascii_letters + " "
            column = pd.DataFrame({"ID":
                                   [i for i in range(count_line)],
                                   name_column:
                                   ["".join([ch(chars)
                                             for i in range(12)])
                                    for j in range(count_line)]})
            return column
        case "int":
            column = pd.DataFrame({"ID": [i for i in range(count_line)],
                                  name_column:
                                  [rd(0, count_line-1) for j in
                                   range(count_line)]})
            return column
        case "datetime":
            column = pd.DataFrame({"ID":
                                   [i for i in range(count_line)],
                                   name_column:
                                   list(dt.fromtimestamp(
                                       rd(1262304000, 1703070053))
                                       for i in range(count_line))})
            return column


if __name__ == "__main__":

    # создаем первую таблицу с ключем "ID" и столбцом "Name"
    table1 = filling_out_data("Name", "object", 20)
    # создаем вторую таблицу с ключем "ID" и столбцом "ID_Campaign"
    table2 = filling_out_data("Campaign_name", "object", 20)
    # создаем третью таблицу с ключем "ID" и столбцами  "ID_Name","ID_Campaign"
    table3 = filling_out_data("ID_Name", "int", 20).merge(
            filling_out_data("ID_Campaign", "int", 20),
            how="left", on="ID")
    # используем БД созданную в postgresql с именем etl для создании таблиц
    #  и последующем их заполением
    engine = create_engine('postgresql://postgres:6161@localhost:5432/etl')
    table3.to_sql('sales', con=engine, index=False, if_exists='replace')
    table1.to_sql('advertisers', con=engine, index=False, if_exists='replace')
    table2.to_sql('campaign', con=engine, index=False, if_exists='replace')
    # Закрываем соединение
    engine.dispose()
    # Выполняем денормализацию таблицы через SQL запрос
    conn = psycopg2.connect('postgresql://postgres:6161@localhost:5432/etl')
    cursor = conn.cursor()

    sql = """ALTER TABLE advertisers
    ADD COLUMN count_compaign INT;
    ALTER TABLE advertisers
    ADD COLUMN count_sales INT;

    UPDATE advertisers
    SET count_compaign = (
    SELECT COUNT(*)
    FROM sales
    WHERE sales."ID_Name" = advertisers."ID"
    );
    UPDATE advertisers
    SET count_sales = (
    SELECT COUNT(*)
    FROM sales,campaign
    WHERE sales."ID_Campaign" = campaign."ID"
    and sales."ID_Name" = advertisers."ID"
    );
    select *
    from advertisers
    """

    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    finally:
        if conn:
            cursor.close()
            conn.close()
    df = pd.merge(table3, table1, how="left", left_on="ID_Name", right_on="ID")
    df = pd.merge(df, table2, how="left", left_on="ID_Campaign", right_on="ID")
    df = df[["ID_x", "Name", 'Campaign_name']].rename(columns={'ID_x': 'ID'})
    c = df.groupby(["Name", "Campaign_name"]).count()["ID"]
    print(c)
