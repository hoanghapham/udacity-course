from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import enum
from helpers.custom_logger import init_logger
from helpers.load_configs import LoadConfig

logger = init_logger(__file__)

class Mode(enum.Enum):
    APPEND_ONLY = "APPEND_ONLY"
    DELETE_INSERT = "DELETE_INSERT"

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id="",
            load_config: LoadConfig = None,
            mode: Mode = Mode.DELETE_INSERT,
            *args, 
            **kwargs
        ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_config = load_config
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        logger.info(f"Loading dimension table {self.load_config.table_name}...")
        
        if self.mode == Mode.DELETE_INSERT:
            redshift.run(self.load_config.drop_table)
        
        redshift.run(self.load_config.create_table)
        redshift.run(self.load_config.insert_table)

