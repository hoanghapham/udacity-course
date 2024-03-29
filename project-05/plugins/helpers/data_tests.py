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
        count(*) as cnt
    from {table}
    where {column} is null
    having cnt > 0
"""

check_has_data_sql = """
    select 
        count(*) as cnt
    from {table}
    having cnt = 0
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
    {'test_name': 'artists_artistid_unique', 'test_sql': check_unique_sql.format(table='artists', column='artistid')},
    {'test_name': 'artists_artistid_not_null', 'test_sql': check_not_null_sql.format(table='artists', column='artistid')},
    {'test_name': 'artists_has_data', 'test_sql': check_has_data_sql.format(table='artists')},

    {'test_name': 'songplays_playid_unique', 'test_sql': check_unique_sql.format(table='songplays', column='playid')},
    {'test_name': 'songplays_playid_not_null', 'test_sql': check_not_null_sql.format(table='songplays', column='playid')},
    {'test_name': 'songplays_has_data', 'test_sql': check_has_data_sql.format(table='songplays')},
    
    {'test_name': 'songs_songid_unique', 'test_sql': check_unique_sql.format(table='songs', column='songid')},
    {'test_name': 'songs_songid_not_null', 'test_sql': check_not_null_sql.format(table='songs', column='songid')},
    {'test_name': 'songs_has_data', 'test_sql': check_has_data_sql.format(table='songs')},
    
    {'test_name': 'users_userid_unique', 'test_sql': check_unique_sql.format(table='users', column='userid')},
    {'test_name': 'users_userid_not_null', 'test_sql': check_not_null_sql.format(table='users', column='userid')},
    {'test_name': 'users_has_data', 'test_sql': check_has_data_sql.format(table='users')},
    
    {'test_name': 'time_start_time_unique', 'test_sql': check_unique_sql.format(table='time', column='start_time')},
    {'test_name': 'time_start_time_not_null', 'test_sql': check_not_null_sql.format(table='time', column='start_time')},
    {'test_name': 'time_has_data', 'test_sql': check_has_data_sql.format(table='time')}
    
]