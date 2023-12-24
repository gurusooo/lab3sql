def execute_request(db_connection, print_flag, sql_request):
    with db_connection.cursor() as cursor_con:
        cursor_con.execute(sql_request)
        if print_flag:
            for i in cursor_con.fetchall():
                print(i)
    db_connection.commit()

def psycopg_q1(con, db_table_name, print_flag = False):
    sql_request = (f"""SELECT 
                   \"VendorID\", count(*) 
                   FROM {db_table_name}
                   GROUP BY 1;""")
    execute_request(con, print_flag, sql_request)

def psycopg_q2(con, db_table_name, print_flag = False):
    sql_request = (f"""SELECT passenger_count, avg(total_amount) 
                   FROM {db_table_name} 
                   GROUP BY 1;""")
    execute_request(con, print_flag, sql_request)

def psycopg_q3(con, db_table_name, print_flag = False):
    sql_request = f"""SELECT 
                    passenger_count,
                    extract(year from tpep_pickup_datetime),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY 1, 2;"""
    execute_request(con, print_flag, sql_request)

def psycopg_q4(con, db_table_name, print_flag = False):
    sql_request = f"""SELECT
                    passenger_count,
                    extract(year from tpep_pickup_datetime),
                    round(trip_distance),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY 1, 2, 3
                    ORDER BY 2, 4 desc;"""
    execute_request(con, print_flag, sql_request)
