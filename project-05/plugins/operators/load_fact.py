from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers.custom_logger import init_logger
from helpers.sql_queries import LoadConfig

logger = init_logger(__file__)

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        load_config: LoadConfig = None,
        *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_config = load_config

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        logger.info(f"Loading fact table {self.load_config.table_name}...")
        
        redshift.run(self.load_config.create_table)
        redshift.run(self.load_config.insert_table)







