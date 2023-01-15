from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import PostgresOperator

import logging

from helpers.sql_queries import StageQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        s3_path = '',
        redshift_conn_id = 'redshift',
        load_config = None,
        aws_credentials_id = 'aws_credentials',
        *args, **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.s3_path = s3_path
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.load_config = load_config

    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        logging.info(f"Staging {self.load_config.table_name}...")

        redshift.run(self.load_config.create_table)
        redshift.run(self.load_config.insert_table.format(
            access_key=credentials.access_key, secret_key=credentials.secret_key))







