from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.custom_logger import init_logger

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
        data_tests,
        *args, 
        **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_tests = data_tests

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        logger.info("Running data quality tests...") 
        
        test_results = []
        failed_tests = []

        # Iterate through all tests, execute, store test result, and pick out failed tests
        for test in self.data_tests:
            
            records = redshift.get_records(test['test_sql'])
            is_passed = (len(records) == 0)

            test_results.append((test['test_name'], is_passed))

            if not is_passed:
                failed_tests.append(test['test_name'])

        if any(failed_tests):
            logger.error(f"Data quality checks failed: {', '.join(failed_tests)}") 
            raise ValueError("Data quality checks failed.")
        else:
            logger.info("All data quality checks passed.")

