import time
import timeit
import psycopg2
import pandas as pd
import duckdb
import sqlite3
from statistics import median
import os
from script_files.getting_data import get_postgres, get_path, get_test, get_settings
from script_files.db_manage import to_postgres_and_sqlite
from script_files.duckdb_q import duckdb_q1, duckdb_q2, duckdb_q3, duckdb_q4
from script_files.pandas_q import pandas_q1, pandas_q2, pandas_q3, pandas_q4
from script_files.psycopg2_q import psycopg_q1, psycopg_q2, psycopg_q3, psycopg_q4
from script_files.sqlite_q import sqlite_q1, sqlite_q2, sqlite_q3, sqlite_q4

path = get_path()
absolute_path = os.path.abspath("../" + path)
# prepare name for table
db_table_name = path.lstrip("data/raw/")
db_table_name = db_table_name.rstrip(".csv")
postgres = get_postgres()
postgres_access = f"postgresql://{postgres['user=']}:{postgres['password=']}@" \
           f"{postgres['host=']}:{postgres['port=']}/{postgres['name=']}"
sqlite_access = f"../data/converted/{db_table_name}.db"

to_postgres_and_sqlite()

tests = get_test()
test_settings = get_settings()
recorded = {"psycopg2=": [[], [], [], []],
                 "sqlite=": [[], [], [], []],
                 "duckdb=": [[], [], [], []],
                 "pandas=": [[], [], [], []],
                 "sqlalchemy=": [[], [], [], []]}

list_to_test = get_test()
#Psycopg2
if list_to_test["psycopg2="]:
    print("\nPsycopg2 Testing started...")
    con = psycopg2.connect(postgres_access)
    for i in range(test_settings["test_count="]):
        start = time()
        psycopg_q1(con, db_table_name, test_settings["query_print="])
        recorded["psycopg2="][0].append(time() - start)

        start = time()
        psycopg_q2(con, db_table_name, test_settings["query_print="])
        recorded["psycopg2="][1].append(time() - start)

        start = time()
        psycopg_q3(con, db_table_name, test_settings["query_print="])
        recorded["psycopg2="][2].append(time() - start)

        start = time()
        psycopg_q4(con, db_table_name, test_settings["query_print="])
        recorded["psycopg2="][3].append(time() - start)
    con.close()
    print("Psycopg2 Testing finished\n")

#SQLite
if list_to_test["sqlite="]:
    print("SQLite Testing started...")
    con = sqlite3.connect(f"data/converted/{db_table_name}.db")
    for i in range(test_settings["test_count="]):
        start = time()
        sqlite_q1(con, db_table_name, test_settings["query_print="])
        recorded["sqlite="][0].append(time() - start)

        start = time()
        sqlite_q2(con, db_table_name, test_settings["query_print="])
        recorded["sqlite="][1].append(time() - start)

        start = time()
        sqlite_q3(con, db_table_name, test_settings["query_print="])
        recorded["sqlite="][2].append(time() - start)

        start = time()
        sqlite_q4(con, db_table_name, test_settings["query_print="])
        recorded["sqlite="][3].append(time() - start)
    con.close()
    print("SQLite Testing finished\n")

#DuckDB
if list_to_test["duckdb="]:
    print("DuckDB Testing started...")
    duckdb.sql("INSTALL postgres")
    duckdb.sql(f"CALL postgres_attach('{postgres_access}')")
    for i in range(test_settings["test_count="]):

        start = time()
        duckdb_q1(db_table_name, test_settings["query_print="])
        recorded["duckdb="][0].append(time() - start)

        start = time()
        duckdb_q2(db_table_name, test_settings["query_print="])
        recorded["duckdb="][1].append(time() - start)

        start = time()
        duckdb_q3(db_table_name, test_settings["query_print="])
        recorded["duckdb="][2].append(time() - start)

        start = time()
        duckdb_q4(db_table_name, test_settings["query_print="])
        recorded["duckdb="][3].append(time() - start)
    print("DuckDB Testing finished\n")

#Pandas
if list_to_test["pandas="]:
    print("Generating file for Pandas")
    pd.options.mode.chained_assignment = None
    engine = psycopg2.create_engine(postgres_access)
    sql = f"SELECT * FROM {db_table_name}"
    df = pd.read_sql(sql, con)
    print("Pandas Testing started...")
    for i in range(test_settings["test_count="]):

        start = time()
        pandas_q1(df, test_settings["print_query="])
        recorded["pandas="][0].append(time() - start)

        start = time()
        pandas_q2(df, test_settings["query_print="])
        recorded["pandas="][1].append(time() - start)

        start = time()
        pandas_q3(df, test_settings["query_print="])
        recorded["pandas="][2].append(time() - start)

        start = time()
        pandas_q4(df, test_settings["query_print="])
        recorded["pandas="][3].append(time() - start)
    engine.dispose()
    del df
    print("Pandas Testing finished\n")

# Plot Bar Chart
queries = ("Q1", "Q2", "Q3", "Q4")
plot_data = {}
max_val = 0
for key in recorded:
    if list_to_test[key]:
        temp_data = [round(median(recorded[key][0]) * 1000, 0), round(median(recorded[key][1]) * 1000, 0),
                     round(median(recorded[key][2]) * 1000, 0), round(median(recorded[key][3]) * 1000, 0)]
        temp_max = max(temp_data)
        max_val = temp_max if temp_max > max_val else max_val
        plot_data.setdefault(key.strip("="), tuple(temp_data))
