import os
from datetime import datetime, timedelta
from airflow import DAG
from helpers import SqlQueries
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

default_args = {
    'owner': 'singaretti',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 4),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
    
}

dag = DAG('sparkify_dag_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

begin_execution = DummyOperator(task_id='begin_execution',  dag=dag)

aws_credentials= "aws_credentials"
redshift_conn = "redshift"

s3_bucket = "s3://udacity-dend"
s3_log_folder = "log_data"
s3_song_folder = "song_data"

drop_tables_before_staging_data = True
dimension_append_mode = True

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events',
    dag = dag,
    redshift_conn_id = redshift_conn,
    s3_path = f"{s3_bucket}/{s3_log_folder}",
    aws_credentials_id = aws_credentials,
    provide_context = True,
    table_name = 'staging_events',
    copy_json_parameter = f"{s3_bucket}/log_json_path.json",
    drop_option = drop_tables_before_staging_data

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_songs',
    dag = dag,
    redshift_conn_id = redshift_conn,
    s3_path = f"{s3_bucket}/{s3_song_folder}",
    aws_credentials_id = aws_credentials,
    provide_context = True,
    table_name = 'staging_songs',
    copy_json_parameter = 'auto',
    drop_option = drop_tables_before_staging_data
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = redshift_conn,
    create_statement=SqlQueries.songplay_create_table,
    select_statement=SqlQueries.songplay_table,
    table_name = 'songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn,
    create_statement=SqlQueries.user_create_table,
    create_tmp_statement = SqlQueries.user_create_table_tmp,
    select_statement=SqlQueries.user_table,
    select_statement_append=SqlQueries.user_table_append,
    table_name = 'users',
    append_mode = dimension_append_mode
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn,
    create_statement=SqlQueries.song_create_table,
    create_tmp_statement = SqlQueries.song_create_table_tmp,
    select_statement=SqlQueries.song_table,
    select_statement_append=SqlQueries.song_table_append,
    table_name = 'songs',
    append_mode = dimension_append_mode
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn,
    create_statement=SqlQueries.artist_create_table,
    create_tmp_statement = SqlQueries.artist_create_table_tmp,
    select_statement=SqlQueries.artist_table,
    select_statement_append=SqlQueries.artist_table_append,
    table_name = 'artists',
    append_mode = dimension_append_mode
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn,
    create_statement=SqlQueries.time_create_table,
    create_tmp_statement = SqlQueries.time_create_table_tmp,
    select_statement=SqlQueries.time_table,
    select_statement_append=SqlQueries.time_table_append,
    table_name = 'time',
    append_mode = dimension_append_mode
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = redshift_conn,
    number_of_rows_to_check = 10,
    tables_in_data_quality_task = ['artists','songs','users','time'],
    columns_in_data_quality_task = ['name','title','first_name','month']
)

end_execution = DummyOperator(task_id='end_execution',  dag=dag)

begin_execution >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_execution