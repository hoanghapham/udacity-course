from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers.load_configs import LoadConfig

from helpers.custom_logger import init_logger

logger = init_logger(__file__)

class StageToRedshiftOperator(BaseOperator):
    """Copy data from S3 to Redshift
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        s3_path = '',
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        load_config: LoadConfig = None,
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

        logger.info(f"Start staging {self.load_config.table_name}...")
        logger.info(f"Deleting table {self.load_config.table_name}...")
        redshift.run(self.load_config.drop_table)

        logger.info(f"Creating table {self.load_config.table_name}...")
        redshift.run(self.load_config.create_table)

        logger.info(f"Inserting data into table {self.load_config.table_name}...")
        redshift.run(self.load_config.insert_table.format(iam_role=credentials.secret_key))

        logger.info(f"Finished staging table {self.load_config.table_name}...")