# lab3sql
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

# Технические характеристики:
* Redmi Book Pro 14 Pro
* Windows/Linux
* Дисковое пространство (использовано 144ГБ из 149ГБ)
* 16,0 ГБ (доступно: 15,2 ГБ)
Язык, среда разработки и пр.
* Pycharm (Python)
* pgAdmin 4
  
<img width="800" alt="tiny_data.png" src="https://github.com/gurusooo/lab3sql/blob/main/script_files/data/tiny_dataset.png">
imagine что тут написано excluding
