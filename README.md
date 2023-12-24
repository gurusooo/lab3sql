# lab3sql
Даны 4 библиотеки: DuckDB, Pandas, Psycopg 2, SQLite
Задачи: написать бенчмарк для измерения скорости выполнения четырёх запросов:
1. SELECT VendorId, count(*) FROM db GROUP BY 1;
2. SELECT passenger_count, avg(total_amount) 
FROM db 
GROUP BY 1;
3. SELECT
   passenger_count, 
   extract(year from pickup_datetime),
   count(*)
FROM db
GROUP BY 1, 2;
4. SELECT
    passenger_count,
    extract(year from pickup_datetime),
    round(trip_distance),
    count(*)
FROM db
GROUP BY 1, 2, 3
ORDER BY 2, 4 desc;

### Технические характеристики:
* Redmi Book Pro 14 Pro
* Windows/Linux
* Дисковое пространство (использовано 144ГБ из 149ГБ)
* 16,0 ГБ (доступно: 15,2 ГБ)
### Язык, среда разработки и пр.
* Pycharm (Python)
* pgAdmin 4
### Необходимые библиотеки:
* DuckDB
* Pandas
* Psycopg 2
* SQLite
# Немного о config файле
*лежит в script_files, название - lib_settings.conf
* test_count - количество запусков
* query_print - печать запросов, false по умолчанию
* csv_file - ссылка на оригинальный csv
* можно "выключить/включить" доступные библиотеки
* для постгреса вводите свои данные
<img width="800" alt="tiny_data.png" src="https://github.com/gurusooo/lab3sql/blob/main/script_files/data/tiny_dataset.png">
imagine что тут написано excluding
+imagine что тут время в мс

### Выводы, мысли
* Самой быстрой оказалась библиотека Pandas, однако она расходует огромное количество оперативной памяти, что делает её крайне неудобной в работе с крупными данными;
* SQLite, напротив, работает медленно, но с меньшим расходом оперативной памяти
* DuckDB выглядит стабильной в работе, каждый запрос обрабатывается за примерно одинаковое время, усложнение запроса практически не влияет на производительность! И умеренный расход оперативки!
* Работа была занятной, довольно непростой, +опыт работы в питоне и с бд (и в гите), интересно)
