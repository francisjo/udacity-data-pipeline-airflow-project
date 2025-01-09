# Custom Operator to execute multiple SQL statements in SQL file in Redshift
# Reference: https://blog.shellkode.com/airflow-postgresql-operator-to-execute-multiple-sql-statements-dd0d07365667

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

# Define the PostgreSQLOperator class
class PostgreSQLOperator(BaseOperator):
    # Template fields for rendering SQL queries from templates
    template_fields = ('sql',)
    template_fields_renderers = {'sql': 'sql'}
    template_ext = ('.sql',)
    # UI color for the operator in the Airflow UI
    ui_color = '#99e699'

    # Constructor to initialize the operator with necessary parameters
    @apply_defaults
    def __init__(self,
                 *,
                 sql: str = '',  # SQL query or list of queries to execute
                 postgres_conn_id: str = 'postgres_default',  # Connection ID for Postgres
                 autocommit: bool = True,  # Whether to autocommit the transaction
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql = sql  # Set the SQL query or queries
        self.postgres_conn_id = postgres_conn_id  # Set the Postgres connection ID
        self.autocommit = autocommit  # Set the autocommit flag

    # Main execution method that runs the SQL statements
    def execute(self, context) -> None:
        # Establish a connection to Postgres using the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Log the start of the SQL execution process
        self.log.info("Starting execution of SQL statements on Redshift")

        # Execute the SQL query or queries
        try:
            if isinstance(self.sql, str):
                postgres_hook.run(self.sql, self.autocommit)
            else:
                for query in self.sql:
                    postgres_hook.run(query, self.autocommit)

            # Log successful completion
            self.log.info("SQL Query Execution completed successfully!")

        except Exception as e:
            # Log the exception if execution fails
            self.log.error(f"SQL Query Execution failed: {str(e)}")
            raise