from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    format as json '{}'
    """

    @apply_defaults
    def __init__(self,
                    redshift_conn = '',
                    aws_credentials = '',
                    table = '',
                    s3_bucket = '',
                    s3_key = '',
                    json_path = '',
                    run_date='',
                    *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.run_date = run_date

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials, client_type='s3')
        self.log.info(aws_hook)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn)

        self.s3_bucket = self.s3_bucket+self.s3_key

        sql = self.COPY_SQL.format(
        self.table,
        self.s3_bucket,
        credentials.access_key,
        credentials.secret_key,
        self.json_path)

        self.log.info(sql)
        redshift_hook.run(sql)
        self.log.info("Stage Complete")
        self.log.info(self.run_date)
