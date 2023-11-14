from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook

class CreateTableOperator(BaseOperator):
    ui_color = '#89DA59'
    

    @apply_defaults
    def __init__(self,
                 aws_credentials,
                redshift_conn_id,
                sql,
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        connection = BaseHook.get_connection(self.redshift_conn_id)
        self.log.info(connection.host)
        self.log.info(connection.port)
        self.log.info(connection.login)
        self.log.info(connection.password)
        self.log.info(connection.schema)
        ls_sql = self.sql.split(';')
        for sql in ls_sql:
            if sql.strip() != '':
                self.log.info(sql)
                redshift.run(sql)

