def execute_request(db_connection, print_flag, sql_request):
    cursor_con = db_connection.cursor()
    cursor_con.execute(sql_request)
    if print_flag:
        for i in cursor_con.fetchall():
            print(i)
    db_connection.commit()

def duckdb_q1(con, db_table_name, print_flag=False):
    sql_request = f"""SELECT \"VendorID\", count(*) 
                    FROM {db_table_name} 
                    GROUP BY \"VendorID\";"""
    execute_request(con, print_flag, sql_request)

def duckdb_q2(con, db_table_name, print_flag=False):
    sql_request = f"""SELECT passenger_count, avg(total_amount) 
                    FROM {db_table_name} 
                    GROUP BY passenger_count;"""
    execute_request(con, print_flag, sql_request)

def duckdb_q3(con, db_table_name, print_flag=False):
    sql_request = f"""SELECT 
                    passenger_count,
                    EXTRACT(YEAR FROM tpep_pickup_datetime),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY passenger_count, EXTRACT(YEAR FROM tpep_pickup_datetime);"""
    execute_request(con, print_flag, sql_request)

def duckdb_q4(con, db_table_name, print_flag=False):
    sql_request = f"""SELECT
                    passenger_count,
                    EXTRACT(YEAR FROM pickup_datetime),
                    round(trip_distance),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY passenger_count, EXTRACT(YEAR FROM pickup_datetime), round(trip_distance)
                    ORDER BY EXTRACT(YEAR FROM pickup_datetime), count(*) desc;"""
    execute_request(con, print_flag, sql_request)
