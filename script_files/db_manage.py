import pandas as pd
import psycopg2
import sqlite3
import csv
from script_files.getting_data import get_path, get_postgres

postgres = get_postgres()
path = get_path()
db_table_name = path.lstrip("data/")
db_table_name = db_table_name.rstrip(".csv")

def to_postgres_and_sqlite():
    data = pd.read_csv(r"" + get_path(), nrows=2)
    headers = list(data.columns)
    for i in range(len(headers)-1, -1, -1):
        temp = headers[i].lower()
        if temp in headers[0:i]:
            headers[i] += "2"
    data_types = {"int64": "int", "float64": "float"}
    column_types = []
    for i, item in enumerate(data.dtypes):
        if "object" in item.name:
            if "time" in headers[i]:
                column_types.append("timestamp")
            elif "flag" in headers[i]:
                column_types.append("varchar(1)")
        else:
            column_types.append(data_types[item.name])
    sql_request_delete_table = f"DROP TABLE IF EXISTS {db_table_name};"
    sql_request_create_table = f"CREATE TABLE {db_table_name}("
    sql_request_copy_data = f"COPY {db_table_name} FROM STDIN DELIMITER ',' CSV HEADER;"
    sqlite_request_insert_records = f"INSERT INTO {db_table_name} VALUES ("
    for i in range(len(headers)):
        sql_request_create_table += f"\"{headers[i]}\" {column_types[i]}"
        if i != len(headers) - 1:
            sql_request_create_table += ", "
            sqlite_request_insert_records += "?, "
        else:
            sql_request_create_table += ");"
            sqlite_request_insert_records += "?)"
    con = psycopg2.connect(
        database=postgres["name"],
        user=postgres["user"],
        password=postgres["password"],
        host=postgres["host"],
        port=postgres["port"]
    )
    con.autocommit = True
    print(f"Copying to PostgreSQL")
    with con.cursor() as cursor:
        cursor.execute(sql_request_delete_table)
        cursor.execute(sql_request_create_table)
        with open(f"" + path, "r") as csv_file:
            cursor.copy_expert(sql=sql_request_copy_data, file=csv_file)
    print(f"Copying to PostgreSQL completed")
    con.commit()
    con.close()
    sqlite_db_path = f"data/converted/{db_table_name}.db"
    file = open(path, "r")
    contents = csv.reader(file)
    next(contents)
    con = sqlite3.connect(sqlite_db_path)
    cursor = con.cursor()
    print(f"Copying to SQLite {db_table_name}.db database")
    cursor.execute(sql_request_delete_table)
    cursor.execute(sql_request_create_table)
    cursor.executemany(sqlite_request_insert_records, contents)
    print(f"Copying to SQLite {db_table_name}.db database")
    file.close()
    con.commit()
    con.close()
