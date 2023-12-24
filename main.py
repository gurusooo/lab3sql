from time import time
import psycopg2
import pandas as pd
import duckdb
import sqlite3
from statistics import median
from script_files.getting_data import get_postgres, get_path, get_test, get_settings
from script_files.db_manage import to_postgres_and_sqlite
from script_files.duckdb_q import duckdb_q1, duckdb_q2, duckdb_q3, duckdb_q4
from script_files.pandas_q import pandas_q1, pandas_q2, pandas_q3, pandas_q4
from script_files.psycopg2_q import psycopg_q1, psycopg_q2, psycopg_q3, psycopg_q4
from script_files.sqlite_q import sqlite_q1, sqlite_q2, sqlite_q3, sqlite_q4

path = get_path()
db_table_name = path.lstrip("data/")
db_table_name = db_table_name.rstrip(".csv")
postgres = get_postgres()
postgres_access = f"postgresql://{postgres['user']}:{postgres['password']}@" \
           f"{postgres['host']}:{postgres['port']}/{postgres['name']}"
sqlite_access = f"data/converted/{db_table_name}.db"

to_postgres_and_sqlite()

tests = get_test()
test_settings = get_settings()
log = {"psycopg2=": [[], [], [], []],
                 "sqlite=": [[], [], [], []],
                 "duckdb=": [[], [], [], []],
                 "pandas=": [[], [], [], []]
       }

list_to_test = get_test()

if list_to_test["duckdb="]:
    print("DuckDB testing is proceeding...")
    duckdb.sql("INSTALL postgres")
    duckdb.sql(f"CALL postgres_attach('{postgres_access}')")
    for i in range(test_settings["test_count="]):

        start = time()
        duckdb_q1(db_table_name, test_settings["query_print="])
        log["duckdb="][0].append(time() - start)

        start = time()
        duckdb_q2(db_table_name, test_settings["query_print="])
        log["duckdb="][1].append(time() - start)

        start = time()
        duckdb_q3(db_table_name, test_settings["query_print="])
        log["duckdb="][2].append(time() - start)

        start = time()
        duckdb_q4(db_table_name, test_settings["query_print="])
        log["duckdb="][3].append(time() - start)
    print("DuckDB testing completed\n")

if list_to_test["pandas="]:
    print("Generating file for Pandas")
    pd.options.mode.chained_assignment = None
    df = pd.read_csv(r"" + get_path())
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

    print("Pandas testing is proceeding...")
    for i in range(test_settings["test_count="]):

        start = time()
        pandas_q1(df, test_settings["query_print="])
        log["pandas="][0].append(time() - start)

        start = time()
        pandas_q2(df, test_settings["query_print="])
        log["pandas="][1].append(time() - start)

        start = time()
        pandas_q3(df, test_settings["query_print="])
        log["pandas="][2].append(time() - start)

        start = time()
        pandas_q4(df, test_settings["query_print="])
        log["pandas="][3].append(time() - start)
    del df
    print("Pandas testing completed\n")

if list_to_test["psycopg2="]:
    print("\nPsycopg2 work is proceeding...")
    con = psycopg2.connect(postgres_access)
    for i in range(test_settings["test_count="]):
        start = time()
        psycopg_q1(con, db_table_name, test_settings["query_print="])
        log["psycopg2="][0].append(time() - start)

        start = time()
        psycopg_q2(con, db_table_name, test_settings["query_print="])
        log["psycopg2="][1].append(time() - start)

        start = time()
        psycopg_q3(con, db_table_name, test_settings["query_print="])
        log["psycopg2="][2].append(time() - start)

        start = time()
        psycopg_q4(con, db_table_name, test_settings["query_print="])
        log["psycopg2="][3].append(time() - start)
    con.close()
    print("Psycopg2 testing completed\n")

if list_to_test["sqlite="]:
    print("SQLite testing is proceeding...")
    con = sqlite3.connect(f"data/converted/{db_table_name}.db")
    for i in range(test_settings["test_count="]):
        start = time()
        sqlite_q1(con, db_table_name, test_settings["query_print="])
        log["sqlite="][0].append(time() - start)

        start = time()
        sqlite_q2(con, db_table_name, test_settings["query_print="])
        log["sqlite="][1].append(time() - start)

        start = time()
        sqlite_q3(con, db_table_name, test_settings["query_print="])
        log["sqlite="][2].append(time() - start)

        start = time()
        sqlite_q4(con, db_table_name, test_settings["query_print="])
        log["sqlite="][3].append(time() - start)
    con.close()
    print("SQLite testing completed\n")


for key in log:
    if list_to_test[key]:
        temp_data = [round(median(log[key][0]) * 1000, 0), round(median(log[key][1]) * 1000, 0),
                     round(median(log[key][2]) * 1000, 0), round(median(log[key][3]) * 1000, 0)]
        temp_max = max(temp_data)
        print(f"Test results for \"{key.strip('=').upper()}\"")
        print(f"{temp_data[0]}\t{temp_data[1]}\t{temp_data[2]}\t{temp_data[3]}\n")
