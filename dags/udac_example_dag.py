from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.models import Variable
from sql_queries import user_table_insert,song_table_insert,artist_table_insert,time_table_insert,songplay_table_insert

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date':datetime(2021,7,9),
    'retries':3,
    'catchup':False,
    'max_retry_delay':timedelta(seconds=300),
    'email_on_retry': False,
     
     
}

dag = DAG('udac_example_dag7',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=Variable.get("s3_backet"),
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=Variable.get("s3_backet"),
    s3_key="song-data/A/A",
    json ="auto",
    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql=songplay_table_insert,
    table="songplays",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    flag="truncate",
    table="users",
    sql=user_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    flag="truncate",
    sql=song_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
     flag="truncate",
    sql=artist_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    flag="append",
    sql=time_table_insert,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
               {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
               
               ],
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    provide_context=True,
   
    
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table 
load_songplays_table >>load_user_dimension_table>>run_quality_checks
load_songplays_table >>load_song_dimension_table>>run_quality_checks
load_songplays_table >>load_artist_dimension_table>>run_quality_checks 
load_songplays_table >>load_time_dimension_table>>run_quality_checks
run_quality_checks >> end_operator