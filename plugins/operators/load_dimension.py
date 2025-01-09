# Import necessary Airflow modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Define the LoadDimensionOperator class
class LoadDimensionOperator(BaseOperator):
    # UI color for the operator in the Airflow UI
    ui_color = '#80BD9E'

    # Constructor to initialize the operator with necessary parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Connection ID for Redshift
                 sql_query="",  # SQL query to insert data
                 table="",  # Target table in Redshift
                 mode="append",  # Load mode: append or truncate-insert
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id  # Set the Redshift connection ID
        self.table = table  # Set the target table
        self.sql_query = sql_query  # Set the SQL query
        self.mode = mode  # Set the load mode

    # Main execution method that runs the data loading process
    def execute(self, context):
        # Establish a connection to Redshift using the PostgresHook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Log the start of the loading process
        self.log.info(f"Starting data load from staging to dimension table: {self.table}")

        # Check if the load mode is truncate-insert
        if self.mode == "truncate-insert":
            self.log.info(f"Truncating the dimension table: {self.table} before inserting data")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        # Log the insertion process
        self.log.info(f"Inserting data into dimension table: {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")

        # Log successful completion
        self.log.info(f"Successfully loaded data into dimension table: {self.table}")
