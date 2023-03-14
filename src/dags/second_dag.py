import contextlib
import hashlib
import pandas as pd
import vertica_python
import json
from typing import Dict, List, Optional
import pendulum
from airflow.decorators import dag, task
import boto3
from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

conn_info = {
    "host": "51.250.75.20",
    "port": 5433,
    "user": "e8eca156yandexby",
    "password":  Variable.get("KEY_3"),
    "database": "dwh"
}

def create_tables_with_download_dates():
    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    with open('/sql/tables_with_download_dates.sql') as f:
        cur.execute(f.read())
    vertica_conn.commit()
    vertica_conn.close()  


def update_global_metrics(ds):
    with open('/sql/update_global_metrics.sql') as f:
        sql = f.read()
        sql = sql.format(execution_date="'"+str(ds)+"'::timestamp")
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        

with DAG (
	"second_dag",
	schedule_interval='@daily',
	start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
	catchup = False,
        tags=['second_dag'],
        is_paused_upon_creation=False
	) as dag:

    crt_with_download_dates = PythonOperator(
    task_id='create_tables_with_download_dates',
    python_callable=create_tables_with_download_dates)

    update_global_metrics = PythonOperator(
    task_id='update_global_metrics',
    python_callable = update_global_metrics,
    op_kwargs = {"ds": '{{ds}}'})

    