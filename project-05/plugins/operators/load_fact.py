from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from plugins.helpers.sql_queries import InsertQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id='',
        redshift_conn_id='redshift',
        query=''
        *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id

        self.redshift_conn_id = redshift_conn_id
        self.query = query


    def execute(self, context):
        # self.log.info('LoadFactOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        redshift.run(
            InsertQueries.stage_table.format(
                table=self.table, 
                source_data=self.source_data, 
                aws_iam_role=self.aws_credentials_id)
        )







