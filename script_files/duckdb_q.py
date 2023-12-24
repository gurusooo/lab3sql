import duckdb
def duckdb_q1(db_table_name, print_flag=False):
    sql_request = f"""SELECT \"VendorID\", count(*) 
                    FROM {db_table_name} 
                    GROUP BY \"VendorID\";"""
    data = duckdb.sql(sql_request).fetchall()
    if print_flag:
        data.show()

def duckdb_q2(db_table_name, print_flag=False):
    sql_request = f"""SELECT passenger_count, avg(total_amount) 
                    FROM {db_table_name} 
                    GROUP BY passenger_count;"""
    data = duckdb.sql(sql_request).fetchall()
    if print_flag:
        data.show()

def duckdb_q3(db_table_name, print_flag=False):
    sql_request = f"""SELECT 
                    passenger_count,
                    EXTRACT(YEAR FROM tpep_pickup_datetime),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY passenger_count, EXTRACT(YEAR FROM tpep_pickup_datetime);"""
    data = duckdb.sql(sql_request).fetchall()
    if print_flag:
        data.show()

def duckdb_q4(db_table_name, print_flag=False):
    sql_request = f"""SELECT
                    passenger_count,
                    EXTRACT(YEAR FROM tpep_pickup_datetime),
                    round(trip_distance),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY passenger_count, EXTRACT(YEAR FROM tpep_pickup_datetime), round(trip_distance)
                    ORDER BY EXTRACT(YEAR FROM tpep_pickup_datetime), count(*) desc;"""
    data = duckdb.sql(sql_request).fetchall()
    if print_flag:
        data.show()


