# Import necessary Airflow modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Define the DataQualityOperator class
class DataQualityOperator(BaseOperator):
    # UI color for the operator in the Airflow UI
    ui_color = '#89DA59'

    # Constructor to initialize the operator with necessary parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Connection ID for Redshift
                 tables="",  # List of tables to perform data quality checks on
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id  # Set the Redshift connection ID
        self.tables = tables  # Set the list of tables to check

    # Main execution method that runs the data quality checks
    def execute(self, context):
        # Establish a connection to Redshift using the PostgresHook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Loop through each table to perform the data quality check
        for table in self.tables:
            self.log.info(f"Starting data quality check for the table: {table}")
            
            # Execute a query to count the number of rows in the table
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            # Check if the query returned any results
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. No results returned for the table: {table}")
            
            # Get the number of rows in the table
            num_records = records[0][0]
            
            # Check if the table contains any rows
            if num_records < 1:
                raise ValueError(f"Data quality check failed. The table {table} contains 0 rows")
            
            # Log a success message if the data quality check passes
            self.log.info(f"Data quality check passed for the table: {table} with {num_records} records found")