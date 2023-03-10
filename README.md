Кирилл, привет!)

1. папка libs содержит файлы для подключения к Postgresql и Vertica. За это отвечают соответствующие папки pg_connect и vertica_connect
2. файл final-dag.py создает таблицы в STAGING (stg) и DWH (cdm), 
выгружает данные в локальные файлы .csv через файл local_loader/repository/local_repository.py и из них загружает в STAGING , используя файл stg_loader/repository/stg_repository.py
3. файл cdm-global_metrics.py запускает sql скрипт sql/cdm/update_global_metrics.sql и обновляет витрину
4. utils/app_config.py - ссылочник на код, utils/utils.py - логгер.
