from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from helpers.sql_queries import StageQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 source_data='',
                 redshift_conn_id = 'redshift',
                 table = '',
                 aws_credentials_id = 'aws_credentials',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.source_data = source_data
        self.aws_credentials_id = aws_credentials_id

        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        # self.log.info('StageToRedshiftOperator not implemented yet')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Staging {self.table}...")

        redshift.run("DELETE FROM {}".format(self.table))

        redshift.run(
            StageQueries.stage_table.format(
                table=self.table, 
                source_data=self.source_data, 
                aws_iam_role=self.aws_credentials_id)
        )






