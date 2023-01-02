'''
Operator for creating tables in Redshift
'''
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    """
        Create Tables operator
            Creates all tables in redshift if not exist.
        
        Args:
            redshift_conn_id: connection name in Airflow to Redshift
            sql_loc: the location of the sql table creates
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                sql_loc = '/home/workspace/airflow/create_tables.sql',
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_loc = sql_loc

    def execute(self, context):

        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info('Create tables in Redshift running. ')

        sql_file = open(self.sql_loc, 'r')
        
        sql_file = sql_file.read()
        
        sql_commands = sql_file.split(';')

        for i, command in enumerate(sql_commands):
            if len(command.strip()) > 1:
                self.log.info(f'Create tables {i+1} running. ')
                redshift.run(command.strip())