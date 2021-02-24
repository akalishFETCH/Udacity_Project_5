import datetime
import logging
#from queries import staging_events_table_create, staging_events_copy

from airflow import DAG, settings
from airflow.models import Variable, Connection
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from libs import sql_statements

conn_id='test'
conn_type='s3'
login='ABC123'
password='ABC123'
host ='12345'
port='12345'

def list_keys(**kwargs):
    hook = S3Hook(aws_conn_id='aws_credentials_personal')
    bucket = 'udacity-dend'
    prefix = 'log_data/2018'
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")
    return bucket, prefix

"""def printer(**kwargs):
    ti = kwargs['ti']
    bucket_name = ti.xcom_pull(task_ids='list_keys')[0]
    logging.info(bucket_name)
    logging.info(sql_statements.songplay_table_create)"""

def connection_maker(**kwargs):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port
    ) #create a connection object
    session = settings.Session() # get the session
    session.add(conn)
    session.commit()

dag = DAG(
        'connect_to_s3',
        start_date=datetime.datetime.now())

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
    )

"""test_push = PythonOperator(
    task_id="test",
    python_callable=printer,
    dag=dag
    )"""

redshift_connection = PythonOperator(
    task_id = 'create_connection_to_redshift',
    python_callable=connection_maker,
    dag=dag
)

list_task >> redshift_connection
