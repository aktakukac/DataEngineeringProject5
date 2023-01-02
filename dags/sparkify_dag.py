'''
DAG for running ETL process
Copy data from S3 to Redshift stage
Create FACT and DIM tables from stage table
Run data quality checks
'''

from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, 
                            LoadFactOperator,
                            LoadDimensionOperator, 
                            DataQualityOperator, 
                            CreateTablesOperator)

from helpers import SqlQueries

# Specify dates to run between
start_date = datetime(2018, 11, 1)
end_date = datetime(2018, 11, 30)

# Specify python callables for start/end
def start():
    # function to log a single line for start
    logging.info("Start of DAG")

def end():
    # function to log a single line for end
    logging.info("End of DAG")

# Specify default arguments
default_args = {
    'owner': 'udacity',
    'start_date': start_date,
    'end_date': end_date,
    'depends_on_past': False,
    'retires': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# Specify and instantiate DAG
dag_name='sparkify_dag'

dag = DAG(dag_name,
        default_args = default_args,
        description = 'Load and transform data in Redshift with Airflow',
        schedule_interval = '@hourly' # schedule_interval='@hourly'
)


# Start dummy operator
start_operator = PythonOperator(
    task_id = 'Begin_execution',
    python_callable = start, 
    dag=dag)


# Create tables in redshift
create_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id='redshift', 
    sql_loc = '/home/workspace/airflow/create_tables.sql'
)

# Staging from S3 to redshift of events
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date,
    truncate = False
)

# Staging from S3 to redshift of songs
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    #s3_key="song_data/A/A/A",
    json_path="auto",
    region="us-west-2",
    file_format="JSON",
    truncate = False
)


# Populate FACT table with songplays
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.songplay_table_insert,
    table='songplays',
    truncate=False
)

# Populate user DIM table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.user_table_insert,
    table='users',
    truncate=False
)



# Populate song DIM table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.song_table_insert,
    table='songs',
    truncate=False
)


# Populate artist DIM table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.artist_table_insert,
    table='artists',
    truncate=False
)

# Populate time DIM table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.time_table_insert,
    table='time',
    truncate=False
)

# Run data quality checks

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag, 
    provide_context=True,
    redshift_conn_id='redshift',
    tables = ["songplays", "artists", "songs", "time","users"],
    checks = [
        {'test_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 
        'expected_result': 0, 
        'comparison': 'eq'},
        {'test_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 
        'expected_result': 0, 
        'comparison': 'eq'},
        {'test_sql': "SELECT COUNT(*) FROM songs", 
        'expected_result': 0, 
        'comparison': 'gt'}]
)

# End dummy operator
end_operator = PythonOperator(
    task_id='Stop_execution', 
    python_callable= end,
    dag=dag)


# Specify task dependencies
start_operator >> create_tables
create_tables >> stage_songs_to_redshift
create_tables >> stage_events_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
