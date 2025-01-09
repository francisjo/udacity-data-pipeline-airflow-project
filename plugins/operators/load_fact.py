# Import necessary Airflow modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Define the LoadFactOperator class
class LoadFactOperator(BaseOperator):
    # UI color for the operator in the Airflow UI
    ui_color = '#F98866'

    # Constructor to initialize the operator with necessary parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Connection ID for Redshift
                 table="",  # Target fact table in Redshift
                 sql_query="",  # SQL query to insert data into the fact table
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id  # Set the Redshift connection ID
        self.table = table  # Set the target fact table
        self.sql_query = sql_query  # Set the SQL query to execute

    # Main execution method that runs the data loading process
    def execute(self, context):
        # Establish a connection to Redshift using the PostgresHook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Log the start of the loading process
        self.log.info(f"Starting data load into the fact table: {self.table}")

        # Execute the SQL query to load data into the fact table
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")

        # Log successful completion
        self.log.info(f"Successfully loaded data into the fact table: {self.table}")
