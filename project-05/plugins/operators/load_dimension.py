from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.custom_logger import init_logger
from helpers.load_configs import LoadConfig
from helpers.settings import LoadMode

logger = init_logger(__file__)

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id="",
            load_config: LoadConfig = None,
            mode: LoadMode = LoadMode.DELETE_INSERT,
            *args, 
            **kwargs
        ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_config = load_config
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        logger.info(f"Start loading dimension table {self.load_config.table_name}")
        
        if self.mode == LoadMode.DELETE_INSERT:
            logger.info(f"Deleting table {self.load_config.table_name}...")
            redshift.run(self.load_config.drop_table)
        
        logger.info(f"Creating table {self.load_config.table_name}...")
        redshift.run(self.load_config.create_table)

        logger.info({F"Inserting data to table {self.load_config.table_name}..."})
        redshift.run(self.load_config.insert_table)

        logger.info(f"Finished loading dimension table {self.load_config.table_name}.")

