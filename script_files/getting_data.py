def get_path():
    with open("script_files\lib_settings.conf", "rt", encoding="utf-8") as file:
        while True:
            t = file.readline().strip()
            if "#csv" in t:
                t_data = file.readline().strip().split()
                if len(t_data) >= 2:
                    return t_data[1]
                else:
                    return None
            if not t:
                break
        return None

def get_postgres():
    data = {"postgresql_enabled": None, "name": None, "user": None, "password": None, "host": None, "port": None}
    with open("script_files\lib_settings.conf", "rt", encoding="utf-8") as file:
        while True:
            t = file.readline().strip()
            if "#postgres" in t:
                for i in range(6):
                    t_data = file.readline().strip().split()
                    config = t_data[0].lower()
                    if config == "postgresql_enabled=":
                        if t_data[1] == "True":
                            data[config] = True
                        else:
                            return None
                    else:
                        data[config] = t_data[1]
                return data

def get_test():
    path_to_csv = get_path()
    if path_to_csv is None:
        print(f"Cannot proceed, scv file not found in config file")
        return None
    data = {"duckdb=": None, "pandas=": None, "psycopg2=": None, "sqlite=": None,}
    with open("settings.conf", "rt", encoding="utf-8") as file:
        while True:
            t = file.readline().strip()
            if "#choose libraries" in t:
                for i in range(5):
                    t_data = file.readline().strip().split()
                    config = t_data[0].lower()
                    data[config] = True if t_data[1] == "True" else False
                break
    postgres_enabled = get_postgres()
    if postgres_enabled is None:
        data["psycopg2="] = False
        data["duckdb="] = False
        data["pandas="] = False
        print(f"Some troubles with PostgreSQL connection...")
    return data

def get_settings():
    data = {"test_count=": None, "query_print=": False}
    with open("lib_settings.conf", "rt", encoding="utf-8") as file:
        while True:
            t = file.readline().strip()
            if "#Configurating" in t:
                for i in range(2):
                    t_data = file.readline().strip().split()
                    config = t_data[0].lower()
                    if config == "query_print=":
                        data[config] = True if t_data[1] == "True" else False
                    else:
                        data[config] = int(t_data[1])
                return data
            if not t:
                break
        return None
