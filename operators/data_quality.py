from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Check Data")
        for table in self.tables:
            sql = f"SELECT COUNT(1) FROM {table};"
            count = redshift.get_records(sql)
            self.log.info(f"Records table {table}: {count}")
