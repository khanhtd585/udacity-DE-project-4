from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    copy_sql = """
        INSERT INTO {}
        ({})
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_load="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_load = sql_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Load data from stage to fact")
        sql = LoadFactOperator.copy_sql.format(self.table, self.sql_load)
        redshift.run(sql)

        self.log.info("Done processing")
