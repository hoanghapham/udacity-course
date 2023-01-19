from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.load_configs import LoadConfig
import logging

class LoadTableOperator(BaseOperator):
    """Base operator to load tables
    """

    def __init__(
            self,
            redshift_conn_id="",
            table_type="",
            load_config: LoadConfig = None,
            logger: logging.Logger = None,
            *args, 
            **kwargs
        ):

        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_config = load_config
        self.table_type = table_type
        self.logger = logger

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.logger.info(f"Loading {self.table_type} table {self.load_config.table_name}...")
        redshift.run(self.load_config.drop_table)
        redshift.run(self.load_config.create_table)
        redshift.run(self.load_config.insert_table)

