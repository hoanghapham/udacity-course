from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.custom_logger import init_logger
from helpers.data_tests import CheckHasData, CheckUnique, CheckNotNull

logger = init_logger(__file__)

class DataQualityOperator(BaseOperator):

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
        
        # Test unique, not null on unique keys of all tables
        # Test has_row for all tables

        artists_tests = [
            CheckUnique(redshift_hook, "artists", "artistid"),
            CheckNotNull(redshift_hook, "artists", "artistid"),
            CheckHasData(redshift_hook, "artists")
        ]

        songplays_tests = [
            CheckUnique(redshift_hook, "songplays", "playid"),
            CheckNotNull(redshift_hook, "songplays", "playid"),
            CheckHasData(redshift_hook, "songplays")
        ]

        songs_tests = [
            CheckUnique(redshift_hook, "songs", "songid"),
            CheckNotNull(redshift_hook, "songs", "songid"),
            CheckHasData(redshift_hook, "songs")
        ]

        users_tests = [
            CheckUnique(redshift_hook, "users", "userid"),
            CheckNotNull(redshift_hook, "users", "userid"),
            CheckHasData(redshift_hook, "users")
        ]

        time_tests = [
            CheckUnique(redshift_hook, "time", "start_time"),
            CheckNotNull(redshift_hook, "time", "start_time"),
            CheckHasData(redshift_hook, "time")
        ]

        artists_results = [(i.test_name, i.execute()) for i in artists_tests]
        songplays_results = [(i.test_name, i.execute()) for i in songplays_tests]
        songs_results = [(i.test_name, i.execute()) for i in songs_tests]
        users_results = [(i.test_name, i.execute()) for i in users_tests]
        time_results = [(i.test_name, i.execute()) for i in time_tests]


        all_tests_results = artists_results + songplays_results + songs_results + users_results + time_results
        
        failed_tests = [test for test, result in all_tests_results if result == 'failed']

        if any(failed_tests):
            logger.error(f"Data quality check failed: {', '.join(failed_tests)}") 
            raise ValueError("Data quality check failed")
        else:
            logger.info("Data quality check passed")

