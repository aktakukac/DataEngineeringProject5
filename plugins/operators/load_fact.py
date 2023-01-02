'''
Operator for loading fact table in redshift
'''

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
        Load Fact operator
            Loads data to Fact table from redshift stage.
        
        Args:
            redshift_conn_id: connection name in Airflow to Redshift
            sql: the sql string that holds the select for source data
            table: table to populate
            truncate: true/false - whether the table should be truncated first
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                sql='',
                table='',
                truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f'Deleting table {self.table} from redshift')
            redshift.run(f'TRUNCATE {self.table}')
        
        redshift.run(f'INSERT INTO {self.table} {self.sql}')
        
        self.log.info(f'Copied {self.table} from redshift stage to fact table.')
