
'''
Operator for loading dimension tables
'''
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):
    """
        Load Dimensions operator
            Loads data to Dimensions table from redshift stage.
        
        Args:
            table: table to populate
            redshift_conn_id: connection name in Airflow to Redshift
            sql: the sql string that holds the select for source data
            truncate: true/false - whether the table should be truncated first
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                table='',
                redshift_conn_id='',
                sql='',
                truncate='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f'Deleting table {self.table} from redshift')
            redshift.run(f'TRUNCATE {self.table}')
        

        redshift.run(f'INSERT INTO {self.table} {self.sql}')
        
        self.log.info(f'Copied {self.table} from redshift stage to dim table.')

