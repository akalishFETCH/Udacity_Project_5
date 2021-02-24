from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    master_sql = """
        INSERT INTO {} ({});
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn="",
                 table="",
                 sql="",
                 insert_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.table = table
        self.sql = sql
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn)
        if self.insert_mode == ' truncate-insert':
            redshift.run("DELETE FROM {}".format(self.table))
            redshift.run(LoadDimensionOperator.master_sql.format(self.table, self.sql))
        else:
            redshift.run(LoadDimensionOperator.master_sql.format(self.table, self.sql))
