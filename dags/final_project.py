# Import necessary modules and libraries
from datetime import datetime, timedelta
import pendulum

# Airflow DAG and operator imports
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator

# Custom operator imports for Redshift and data quality checks
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

# SQL queries from helper module
from helpers.sql_queries import SqlQueries
# Airflow Variable for dynamic values
from airflow.models import Variable

# Default arguments for the DAG
# These define common settings for all tasks in the DAG
default_args = {
    'owner': 'francisjo',  # Owner of the DAG
    'depends_on_past': False,  # Task doesn't depend on past runs
    'start_date': pendulum.now(),  # Start date for the DAG
    'retries': 3,  # Number of retries for failed tasks
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'catchup': False,  # Prevents backfilling past runs
    'email_on_retry': False  # Disable email notifications on retry
}

# Define the DAG using the @dag decorator
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval= '@hourly' # Schedule to run once an hour
)
def final_project():
    # Fetching Airflow Variables for S3 bucket and prefix paths
    s3_bucket = Variable.get('s3_bucket')  # S3 bucket name
    log_data_prefix = Variable.get('s3_prefix_log_data')  # Prefix for log data in S3
    song_data_prefix = Variable.get('s3_prefix_song_data')  # Prefix for song data in S3
    log_json_path_prefix = Variable.get('s3_prefix_log_json_path')  # Path to JSON format file in S3

    # Dummy start task to signify the beginning of the DAG execution
    start_operator = DummyOperator(task_id='Begin_execution')

    # Stage events data from S3 to Redshift staging table
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',  # Target Redshift table
        redshift_conn_id='redshift',  # Redshift connection ID in Airflow
        aws_credentials_id='aws_credentials',  # AWS credentials ID in Airflow
        s3_bucket=s3_bucket,  # S3 bucket containing the data
        s3_key=log_data_prefix,  # S3 key (prefix) for log data
        log_json_file=log_json_path_prefix  # JSON format file for log data
    )

    # Stage songs data from S3 to Redshift staging table
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',  # Target Redshift table
        redshift_conn_id='redshift',  # Redshift connection ID in Airflow
        aws_credentials_id='aws_credentials',  # AWS credentials ID in Airflow
        s3_bucket=s3_bucket,  # S3 bucket containing the data
        s3_key=song_data_prefix  # S3 key (prefix) for song data
    )

    # Load the songplays fact table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',  # Redshift connection ID in Airflow
        table='songplays',  # Fact table in Redshift
        sql_query=SqlQueries.songplay_table_insert  # SQL query to insert data into songplays
    )

    # Load the users dimension table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',  # Redshift connection ID in Airflow
        table='users',  # Dimension table in Redshift
        sql_query=SqlQueries.user_table_insert,  # SQL query to insert data into users table
        mode='truncate-insert'  # Load mode: truncate before insert
    )

    # Load the songs dimension table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',  # Redshift connection ID in Airflow
        table='songs',  # Dimension table in Redshift
        sql_query=SqlQueries.song_table_insert,  # SQL query to insert data into songs table
        mode='truncate-insert'  # Load mode: truncate before insert
    )

    # Load the artists dimension table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',  # Redshift connection ID in Airflow
        table='artists',  # Dimension table in Redshift
        sql_query=SqlQueries.artist_table_insert,  # SQL query to insert data into artists table
        mode='truncate-insert'  # Load mode: truncate before insert
    )

    # Load the time dimension table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',  # Redshift connection ID in Airflow
        table='time',  # Dimension table in Redshift
        sql_query=SqlQueries.time_table_insert,  # SQL query to insert data into time table
        mode='truncate-insert'  # Load mode: truncate before insert
    )

    # Run data quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',  # Redshift connection ID in Airflow
        tables=['songplays', 'users', 'songs', 'artists', 'time']  # Tables to check for data quality
    )

    # Dummy end task to signify the end of the DAG execution
    end_operator = DummyOperator(task_id='Stop_execution')

    # Define task dependencies to set the order of execution
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
        load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
        run_quality_checks >> end_operator

# Instantiate the DAG
final_project_dag = final_project()
