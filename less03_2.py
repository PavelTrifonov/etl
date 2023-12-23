# В базе данных есть две таблицы: страны и клиенты. Одной из потребностей
# компании является исследование клиентов и стран с точки зрения
# эффективности продаж, поэтому часто выполняются объединения между таблицами:
#  клиенты и страны. Что нужно сделать, чтобы ограничить частое объединение
#  этих двух таблиц?
from less03_1 import filling_out_data as fud
import psycopg2
from sqlalchemy import create_engine


# создаем первую таблицу с ключем "ID" и столбцом "country_name""
table1 = fud("country_name", "object", 20)

# создаем вторую таблицу с ключем "ID", столбцом "customer_name"
# и внешним ключе 'country_id'
table2 = fud('customer_name', 'object', 20).merge(
         fud('country_id', 'int', 20),
         how="left", on="ID")

if __name__ == "__main__":
    # используем БД созданную в postgresql с именем etl для создании таблиц
    #  и последующем их заполением
    engine = create_engine('postgresql://postgres:6161@localhost:5432/etl')
    table1.to_sql('countries', con=engine, index=False, if_exists='replace')
    table2.to_sql('customers', con=engine, index=False, if_exists='replace')
    # Закрываем соединение
    engine.dispose()
    # Создадим материализованное представление (Materialized View), которое
    #  представляет собой реальное физическое хранилище данных на основе
    #  результата объединения. Это позволит нам выполнять запросы к этому
    #  представлению, обновляя его периодически.
    conn = psycopg2.connect('postgresql://postgres:6161@localhost:5432/etl')
    cursor = conn.cursor()

    sql = """CREATE MATERIALIZED VIEW client_country_data AS
SELECT
    customers."ID" AS client_id,
    customers.customer_name AS client_name,
    countries."ID" AS country_id,
    countries.country_name AS country_name
FROM
    customers
JOIN
    countries ON customers.country_id = countries."ID";
    """
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    sql2 = """select *
from public.client_country_data"""
    conn = psycopg2.connect('postgresql://postgres:6161@localhost:5432/etl')
    cursor = conn.cursor()
    cursor.execute(sql2)
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
    conn.close()
