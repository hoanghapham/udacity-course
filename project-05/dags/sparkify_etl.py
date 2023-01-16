from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from plugins.helpers.sql_queries import (
    StageEventsTable,
    StageSongsTable,
    LoadArtistsDimTable,
    LoadSongsDimTable,
    LoadUsersDimTable,
    LoadTimeDimTable,
    LoadSongplaysFactTable
)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_logs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_logs',
    dag=dag,
    s3_path='s3://udacity-dend/log_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    load_config=StageEventsTable
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_path='s3://udacity-dend/song_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    load_config=StageSongsTable
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    load_config=LoadSongplaysFactTable
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    load_config=LoadUsersDimTable
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    load_config=LoadSongsDimTable
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    load_config=LoadArtistsDimTable
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    load_config=LoadTimeDimTable
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

stage_logs_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator