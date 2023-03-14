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
 

AWS_ACCESS_KEY_ID =  Variable.get("KEY")
AWS_SECRET_ACCESS_KEY =  Variable.get("KEY_2")

conn_info = {
    "host": "51.250.75.20",
    "port": 5433,
    "user": "e8eca156yandexby",
    "password":  Variable.get("KEY_3"),
    "database": "dwh"
}

session = boto3.session.Session()
s3_client = session.client(
service_name='s3',
endpoint_url='https://storage.yandexcloud.net',
aws_access_key_id=AWS_ACCESS_KEY_ID,
aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def create_tables():
    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    with open('/sql/tables.sql') as f:
        cur.execute(f.read())
    vertica_conn.commit()
    vertica_conn.close()    


def loading_buckets_transactions(Bucket: str):
    for i in range(1,11):
        s3_client.download_file(
        Bucket=Bucket,
        Key=f'transactions_batch_{i}.csv',
        Filename=f'/data/transactions_batch_{i}.csv') 


def loading_bucket_currencies(Bucket:str):
    s3_client.download_file(
    Bucket=Bucket,
    Key='currencies_history.csv',
    Filename=f'/data/currencies_history.csv') 


def load_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str]):
    df = pd.read_csv(dataset_path)
    if table == 'currencies':
        df = df[['date_update', 'currency_code', 'currency_code_with', 'currency_with_div']]
    num_rows = len(df)
    vertica_conn = vertica_python.connect(**conn_info)
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/data/chunk.csv', index=False)
            with open('/data/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()


def load_to_transactions(schema: str, table: str, columns: List[str]):
    for i in range(1,11):
        load_to_vertica(dataset_path=f'/data/transactions_batch_{i}.csv',
                        schema=schema,
                        table=table,
                        columns=columns)



with DAG (
	"first_dag",
	schedule_interval='0 0 * * *',
	start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
	catchup = False,
        tags=['first_dag'],
        is_paused_upon_creation=False
	) as dag:

    create_tables = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables)

    loading_buckets_transactions = PythonOperator(
	task_id = 'loading_buckets_transactions',
	python_callable = loading_buckets_transactions,
    	op_kwargs = {'Bucket':'final-project'})
    
    loading_bucket_currencies = PythonOperator(
	task_id = 'loading_bucket_currencies',
	python_callable = loading_bucket_currencies,
    	op_kwargs = {'Bucket':'final-project'})


    load_to_transactions = PythonOperator(
        task_id='load_to_transactions',
        python_callable=load_to_transactions,
        op_kwargs={
            'schema': 'E8ECA156YANDEXBY__STAGING',
            'table': 'transactions',
            'columns': ['operation_id', 'account_number_from', 'account_number_to', 'currency_code', 'country', 'status', 'transaction_type', 'amount', 'transaction_dt'],
        },
    )


    load_to_currencies = PythonOperator(
        task_id='load_to_currencies',
        python_callable=load_to_vertica,
        op_kwargs={
            'dataset_path':'/data/currencies_history.csv',
            'schema': 'E8ECA156YANDEXBY__STAGING',
            'table': 'currencies',
            'columns': ['date_update', 'currency_code', 'currency_code_with', 'currency_with_div'],
        },
    )
