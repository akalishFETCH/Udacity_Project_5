from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins.helpers.sql_queries import *
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn)
        songplays_nulls = redshift.get_records(SqlQueries.songplays_nulls)
        logging.info(songplays_nulls)
        user_nulls = redshift.get_records(SqlQueries.user_nulls)
        logging.info(user_nulls)
        song_nulls = redshift.get_records(SqlQueries.song_nulls)
        logging.info(song_nulls)
        artist_nulls = redshift.get_records(SqlQueries.artist_nulls)
        logging.info(artist_nulls)
        time_nulls = redshift.get_records(SqlQueries.time_nulls)
        logging.info(time_nulls)
