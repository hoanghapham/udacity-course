
class DataQualityQueries:
    check_unique_sql = """
        sellect
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

    check_has_data = """
        select 
            count(*) as cnt
        from {table}
        having cnt > 0
    """

def check_unique(redshift_hook, table, column):
    check_unique_sql = """
        sellect
            {column},
            count(*) as cnt
        from {table}
        group by 1
        having cnt > 1
    """
