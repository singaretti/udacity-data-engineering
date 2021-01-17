import os
from datetime import datetime, timedelta
from airflow import DAG
from helpers import SqlQueries
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadFactOperator, LoadDimensionOperator, DataQualityOperator)

default_args = {
    'owner': 'singaretti',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False

}

dag = DAG('covid-analysis',
          default_args=default_args,
          description='Load and transform covid-19 data in Redshift with Airflow',
          schedule_interval='0 3 * * *'
        )

begin_execution = DummyOperator(task_id='Begin_execution',  dag=dag)

redshift_conn = "redshift"

dimension_append_mode = False

load_covid_analysis_table = LoadFactOperator(
    task_id='Load_covid_analysis_fact_table',
    dag=dag,
    redshift_conn_id = redshift_conn,
    create_statement=SqlQueries.covid_analysis_create_table,
    select_statement=SqlQueries.covid_analysis_table,
    table_name = 'covid_analysis'
)

load_city_dimension_table = LoadDimensionOperator(
    task_id='Load_city_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn,
    create_statement=SqlQueries.city_create_table,
    create_tmp_statement = SqlQueries.city_create_table_tmp,
    select_statement=SqlQueries.city_table,
    select_statement_append=SqlQueries.city_table_append,
    table_name = 'city',
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

run_quality_first_check = DataQualityOperator(
    task_id='Run_data_quality_first_check',
    dag=dag,
    redshift_conn_id = redshift_conn,
    number_of_rows_to_check = 10,
    tables_in_data_quality_task = ['staging_italy_regioni','staging_italy_province','staging_germany'],
    columns_in_data_quality_task = ['dt','dt','dt']
)

run_quality_final_check = DataQualityOperator(
    task_id='Run_data_quality_final_check',
    dag=dag,
    redshift_conn_id = redshift_conn,
    number_of_rows_to_check = 5,
    tables_in_data_quality_task = ['covid_analysis','city','time'],
    columns_in_data_quality_task = ['cases','city_name','weekday']
)

end_execution = DummyOperator(task_id='End_execution', dag=dag)

begin_execution >> run_quality_first_check
run_quality_first_check >> load_covid_analysis_table
load_covid_analysis_table >> [load_city_dimension_table, load_time_dimension_table]
[load_city_dimension_table, load_time_dimension_table] >> run_quality_final_check
run_quality_final_check >> end_execution