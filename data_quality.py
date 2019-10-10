from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Data Quality Check For {table} Table")
            rows = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(rows) < 1 or len(rows[0]) < 1:
                raise ValueError(f"Data Quality Check Has Failed. {table} Has Returned No Results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data Quality Check Has Failed. {table} Contained 0 Rows")
            self.log.info(f"Data Quality on The {table} Table Has Passed With {records[0][0]} Rows")