from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from helpers.sql_queries import StageQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        s3_bucket='',
        s3_key='',
        redshift_conn_id = 'redshift',
        table = '',
        aws_credentials_id = 'aws_credentials',
        *args, **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id

        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Staging {self.table}...")     
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageQueries.stage_table.format(
                table=self.table, 
                s3_path=s3_path,
                access_key=credentials.access_key,
                secret_key=credentials.secret_key
        )
        
        redshift.run(formatted_sql)






