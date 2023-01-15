from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.custom_logger import init_logger

logger = init_logger(__file__)

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(
        self, 
        redshift_conn_id: str, 
        sql: str, 
        table: str, 
        column: str = None, 
        *args, 
        **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.column = column
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        result = redshift_hook.get_records(self.sql.format(table=self.table, column=self.column))
        
        if len(result) > 0 or len(result[0]) > 0:
            logger.error(f"Data quality check failed. Quality check query: {self.sql}")
            raise ValueError("Data quality check failed.")
        else:
            logger.info("Data quality check passed.")

