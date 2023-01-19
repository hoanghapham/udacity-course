from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers.load_configs import (
    StageEventsTable,
    StageSongsTable,
    LoadArtistsDimTable,
    LoadSongsDimTable,
    LoadUsersDimTable,
    LoadTimeDimTable,
    LoadSongplaysFactTable
)

from helpers.settings import LoadMode


default_args = {
    'owner': 'hoanghapham',
    'start_date': datetime(2023, 1, 19),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkfy_etl',
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
    load_config=LoadUsersDimTable,
    mode=LoadMode.DELETE_INSERT
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    load_config=LoadSongsDimTable,
    mode=LoadMode.DELETE_INSERT
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    load_config=LoadArtistsDimTable,
    mode=LoadMode.DELETE_INSERT
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    load_config=LoadTimeDimTable,
    mode=LoadMode.DELETE_INSERT
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [
    stage_logs_to_redshift, 
    stage_songs_to_redshift
] >> load_songplays_table

load_songplays_table >> [
    load_songs_dimension_table, 
    load_user_dimension_table, 
    load_artists_dimension_table, 
    load_time_dimension_table
] >> run_quality_checks 

run_quality_checks >> end_operator