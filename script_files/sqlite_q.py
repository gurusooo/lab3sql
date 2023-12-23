def execute_request(db_connection, print_flag, sql_request):
    cursor_con = db_connection.cursor()
    cursor_con.execute(sql_request)
    if print_flag:
        for i in cursor_con.fetchall():
            print(i)
    db_connection.commit()

def sqlite_q1(con, db_table_name, print_flag=False):
    sql_request = f"""SELECT \"VendorID\", count(*) 
                    FROM {db_table_name} 
                    GROUP BY 1;"""
    execute_request(con, print_flag, sql_request)

def sqlite_q2(con, db_table_name, print_flag=False):
    sql_request = (f"""SELECT passenger_count, avg(total_amount) "
                   FROM {db_table_name} 
                   GROUP BY 1;""")
    execute_request(con, print_flag, sql_request)

def sqlite_q3(con, db_table_name, print_flag=False):
    sql_request = f"""SELECT 
                    passenger_count,
                    strftime('%Y', tpep_pickup_datetime),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY 1, 2;"""
    execute_request(con, print_flag, sql_request)

def sqlite_q4(con, db_table_name, print_flag=False):
    sql_request = f"""SELECT
                    passenger_count,
                    strftime('%Y', pickup_datetime),
                    round(trip_distance),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY 1, 2, 3
                    ORDER BY 2, 4 desc;"""
    execute_request(con, print_flag, sql_request)
