from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.custom_logger import init_logger
from helpers.data_tests import CheckHasData, CheckUnique, CheckNotNull

logger = init_logger(__file__)

class DataQualityOperator(BaseOperator):
    """Iterate over a set of tests for all tables, and output a list of failed test if any.

    Parameters
    ----------

    Raises
    ------
    ValueError
        Raised if there are any failed test
    """

    ui_color = '#89DA59'

    def __init__(
        self, 
        redshift_conn_id: str, 
        *args, 
        **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Specify tests for all tables
        tests = [
            CheckUnique(redshift_hook, "artists", "artistid"),
            CheckNotNull(redshift_hook, "artists", "artistid"),
            CheckHasData(redshift_hook, "artists"),
            
            CheckUnique(redshift_hook, "songplays", "playid"),
            CheckNotNull(redshift_hook, "songplays", "playid"),
            CheckHasData(redshift_hook, "songplays"),

            CheckUnique(redshift_hook, "songs", "songid"),
            CheckNotNull(redshift_hook, "songs", "songid"),
            CheckHasData(redshift_hook, "songs"),

            CheckUnique(redshift_hook, "users", "userid"),
            CheckNotNull(redshift_hook, "users", "userid"),
            CheckHasData(redshift_hook, "users"),

            CheckUnique(redshift_hook, "time", "start_time"),
            CheckNotNull(redshift_hook, "time", "start_time"),
            CheckHasData(redshift_hook, "time")
        ]

        # Iterate through all tests, execute, and pick out failed tests
        results = [(i.test_name, i.execute()) for i in tests]
        failed_tests = [test for test, result in results if result == 'failed']

        if any(failed_tests):
            logger.error(f"Data quality check failed: {', '.join(failed_tests)}") 
            raise ValueError("Data quality check failed")
        else:
            logger.info("Data quality check passed")

