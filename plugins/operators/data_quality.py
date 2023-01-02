'''
Operator for running basic data quality checks
'''
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
        Data Quality operator
            Checks whether the tables in arg contains data at all.
        
        Args:
            redshift_conn_id: connection name in Airflow to Redshift
            tables: list tables to check
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                tables=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            formatted_sql = f"SELECT COUNT(*) FROM {table}"
            records = redshift.get_records(formatted_sql)
            num_records = records[0][0]
            
            if num_records < 1 :
                raise ValueError(f"Data quality check failed for {table}")

            self.log.info(f"Data quality on table {table} check passed with {num_records} records")