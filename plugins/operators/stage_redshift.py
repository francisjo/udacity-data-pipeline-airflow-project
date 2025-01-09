# Import necessary Airflow modules
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Define the StageToRedshiftOperator class
class StageToRedshiftOperator(BaseOperator):
    # UI color for the operator in the Airflow UI
    ui_color = '#358140'

    # SQL template for the COPY command
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    # Constructor to initialize the operator with necessary parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Connection ID for Redshift
                 aws_credentials_id="",  # AWS credentials ID in Airflow
                 table="",  # Target table in Redshift
                 s3_bucket="",  # S3 bucket containing the data
                 s3_key="",  # S3 key (prefix) for the data
                 log_json_file="",  # Path to the JSON format file
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table  # Set the target table
        self.redshift_conn_id = redshift_conn_id  # Set the Redshift connection ID
        self.s3_bucket = s3_bucket  # Set the S3 bucket
        self.s3_key = s3_key  # Set the S3 key (prefix)
        self.log_json_file = log_json_file  # Set the path to the JSON format file
        self.aws_credentials_id = aws_credentials_id  # Set the AWS credentials ID

    # Main execution method that runs the staging process
    def execute(self, context):
        # Get AWS credentials using the AwsHook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # Establish a connection to Redshift using the PostgresHook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Log the start of the data clearing process
        self.log.info(f"Starting the process: Clearing data from the Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Log the start of the data copy process
        self.log.info(f"Initiating data transfer from S3 to Redshift for table {self.table}")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}/"

        # Format the COPY command based on whether a JSON file is provided
        if self.log_json_file:
            self.log_json_file = f"s3://{self.s3_bucket}/{self.log_json_file}"
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.log_json_file
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                "auto"
            )

        # Execute the formatted SQL command in Redshift
        redshift.run(formatted_sql)

        # Log successful completion
        self.log.info(f"Data transfer from S3 to Redshift completed successfully for table {self.table}")
