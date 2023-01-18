
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

    def __init__(self, redshift_hook, table, column) -> None:
        super().__init__(redshift_hook, check_unique_sql.format(table=table, column=column))
        self.test_name = f"{table}_{column}_unique"

class CheckNotNull(DataTest):

    def __init__(self, redshift_hook, table, column) -> None:
        super().__init__(redshift_hook, check_not_null_sql.format(table=table, column=column))
        self.test_name = f"{table}_{column}_not_null"

class CheckHasData(DataTest):

    def __init__(self, redshift_hook, table) -> None:
        super().__init__(redshift_hook, check_has_data_sql.format(table=table))
        self.test_name = f"{table}_has_data"
