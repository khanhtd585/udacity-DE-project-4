from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as json '{}'
        STATUPDATE ON
        region '{}';
    """

    @apply_defaults
    def __init__(self, 
                redshift_conn_id="",
                aws_credentials=None,
                table="",
                s3_bucket="",
                s3_key="",
                region='us-west-2',
                format_json='auto',
                *args, **kwargs):
    

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region_ = region
        self.format_json = format_json

    def execute(self, context: Context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id, )

        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table};")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.format_json,
            self.region_,
        )
        print(formatted_sql)
        redshift.run(formatted_sql)
        self.log.info("Done processing")
