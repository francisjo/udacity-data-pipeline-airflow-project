# Import necessary modules and libraries
from datetime import datetime, timedelta
import pendulum
import os

# Airflow DAG and operator imports
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator

# Import custom Redshift operator
from operators.redshift_custom_operator import PostgreSQLOperator

# Default arguments for the DAG
# These define common settings for all tasks in the DAG
default_args = {
    'owner': 'francisjo',  # Owner of the DAG
    'start_date': pendulum.now(),  # Start date for the DAG
    'depends_on_past': False,  # Task doesn't depend on past runs
    'retries': 3,  # Number of retries for failed tasks
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'catchup': False,  # Prevents backfilling past runs
    'email_on_retry': False  # Disable email notifications on retry
}

# Define the DAG using the @dag decorator
@dag(
    default_args=default_args,
    description='Create tables in Redshift with Airflow',
    schedule_interval='0 * * * *'  # Schedule to run once an hour
)
def create_tables():
    # Dummy start task to signify the beginning of the DAG execution
    start_operator = DummyOperator(task_id='Begin_execution')

    # Task to execute SQL script to create tables in Redshift
    create_redshift_tables = PostgreSQLOperator(
        task_id='Create_tables',
        postgres_conn_id='redshift',  # Redshift connection ID in Airflow
        sql='create_tables.sql'  # SQL script to run
    )

    # Dummy end task to signify the end of the DAG execution
    end_operator = DummyOperator(task_id='Stop_execution')

    # Define task dependencies to set the order of execution
    start_operator >> create_redshift_tables >> end_operator

# Instantiate the DAG
create_tables_dag = create_tables()