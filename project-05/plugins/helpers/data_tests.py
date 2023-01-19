check_unique_sql = """
    select
        {column},
        count(*) as cnt
    from {table}
    group by 1
    having cnt > 1
"""

check_not_null_sql = """
    select
        count(case when {column} is null then 1 else null end) as cnt
    from {table}
    having cnt > 0
"""

check_has_data_sql = """
    select 
        count(*) as cnt
    from {table}
    having cnt == 0
"""

class DataTest:
    """Run a data test SQL. 
    If the SQL returns at least one record, flag the test as failed.
    If it returns no result, flag the test as `passed`
    """

    def __init__(self,redshift_hook, sql) -> None:
        self.sql = sql
        self.redshift_hook = redshift_hook
    
    def execute(self):
        records = self.redshift_hook.get_records(self.sql)
        if len(records) > 0:
            return 'failed'
        else:
            return 'passed'


class CheckUnique(DataTest):
    """Check if the value of a column in a table is unique
    """

    def __init__(self, redshift_hook, table, column) -> None:
        super().__init__(redshift_hook, check_unique_sql.format(table=table, column=column))
        self.test_name = f"{table}_{column}_unique"

class CheckNotNull(DataTest):
    """Check if there are any NULL in a column of a table
    """

    def __init__(self, redshift_hook, table, column) -> None:
        super().__init__(redshift_hook, check_not_null_sql.format(table=table, column=column))
        self.test_name = f"{table}_{column}_not_null"

class CheckHasData(DataTest):
    """Check if the table has data or not
    """

    def __init__(self, redshift_hook, table) -> None:
        super().__init__(redshift_hook, check_has_data_sql.format(table=table))
        self.test_name = f"{table}_has_data"


data_tests = [
    {'test_name': 'unique_artists_artistid', 'test_sql': check_unique_sql.format(table='artists', column='artistid'), 'expected_result': 0},
    {'test_name': 'not_null_artists_artistid', 'test_sql': check_not_null_sql.format(table='artists', column='artistid'), 'expected_result': 0},
    {'test_name': 'has_data_artists', 'test_sql': check_has_data_sql.format(table='artists'), 'expected_result': 0},

    {'test_name': 'unique_songplays_playid', 'test_sql': check_unique_sql.format(table='songplays', column='playid'), 'expected_result': 0},
    {'test_name': 'not_null_songplays_playid', 'test_sql': check_not_null_sql.format(table='songplays', column='playid'), 'expected_result': 0},
    {'test_name': 'has_data_songplays', 'test_sql': check_has_data_sql.format(table='songplays'), 'expected_result': 0},
    
    {'test_name': 'unique_songs_songid', 'test_sql': check_unique_sql.format(table='songs', column='songid'), 'expected_result': 0},
    {'test_name': 'not_null_songs_songid', 'test_sql': check_not_null_sql.format(table='songs', column='songid'), 'expected_result': 0},
    {'test_name': 'has_data_songs', 'test_sql': check_has_data_sql.format(table='songs'), 'expected_result': 0},
    
    {'test_name': 'unique_users_userid', 'test_sql': check_unique_sql.format(table='users', column='userid'), 'expected_result': 0},
    {'test_name': 'not_null_users_userid', 'test_sql': check_not_null_sql.format(table='users', column='userid'), 'expected_result': 0},
    {'test_name': 'has_data_users', 'test_sql': check_has_data_sql.format(table='users'), 'expected_result': 0},
    
    {'test_name': 'unique_time_start_time', 'test_sql': check_unique_sql.format(table='time', column='start_time'), 'expected_result': 0},
    {'test_name': 'not_null_time_start_time', 'test_sql': check_not_null_sql.format(table='time', column='start_time'), 'expected_result': 0},
    {'test_name': 'has_data_time', 'test_sql': check_has_data_sql.format(table='time'), 'expected_result': 0}
    
]