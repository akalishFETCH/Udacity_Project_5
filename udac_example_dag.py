from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.stage_redshift import StageToRedshiftOperator
from airflow.operators.load_fact import LoadFactOperator
from airflow.operators.load_dimension import LoadDimensionOperator
from airflow.operators.data_quality import DataQualityOperator
from airflow.Helpers.sql_queries import *
from airflow.operators.postgres_operator import PostgresOperator
import logging

AWS_KEY = os.environ.get('AWS_KEY_PERSONAL')
AWS_SECRET = os.environ.get('AWS_SECRET_PERSONAL')

dag = DAG('sparkify-etl',
          start_date=datetime.now()
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Only on the first run
"""create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift
)"""

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="load_events_from_s3_to_redshift",
    dag=dag,
    table="staging_events",
    redshift_conn="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="s3://udacity-dend",
    s3_key="/log_data",
    json_path='s3://udacity-dend/log_json_path.json',
    run_date='2021-01-01'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="load_songs_from_s3_to_redshift",
    dag=dag,
    table="staging_songs",
    redshift_conn="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="s3://udacity-dend",
    s3_key="/song_data/A/A/B/",
    json_path='auto',
    run_date='2021-01-01'
)

load_songplays_table = LoadFactOperator(
    task_id="load_songplays_from_staging_tables",
    dag=dag,
    table="songplays",
    redshift_conn="redshift",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn="redshift",
    sql=SqlQueries.user_table_insert,
    insert_mode='truncate-insert'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn="redshift",
    sql=SqlQueries.song_table_insert,
    insert_mode='truncate-insert'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn="redshift",
    sql=SqlQueries.artist_table_insert,
    insert_mode='truncate-insert'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn="redshift",
    sql=SqlQueries.time_table_insert,
    insert_mode='truncate-insert'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
load_songplays_table << stage_events_to_redshift, stage_songs_to_redshift
load_user_dimension_table << stage_events_to_redshift
load_song_dimension_table << stage_songs_to_redshift
load_artist_dimension_table << stage_songs_to_redshift
load_time_dimension_table << stage_events_to_redshift
run_quality_checks << load_songplays_table, load_song_dimension_table,\
load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table
end_operator << run_quality_checks
