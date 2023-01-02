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
            tables: list tables to check for null recordsd
            checks: specific SQL queries to run against the tables
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                tables=[],
                checks=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            formatted_sql = f"SELECT COUNT(*) FROM {table}"
            records = redshift.get_records(formatted_sql)
            num_records = records[0][0]
            
            if len(records) < 1 or len(records[0]) < 1 or num_records < 1 :
                raise ValueError(f"Data quality check failed for {table}")

            self.log.info(f"Data quality on table {table} check passed with {num_records} records")
            
        for i, check in enumerate(self.checks):
            sql = check.get('test_sql')
            exp_result = check.get('expected_result')
            operator = check.get('comparison')

            records = redshift.get_records(sql)
            num_records = records[0][0]
    
            if operator == 'eq':
                # equal
                if not num_records == exp_result:
                    raise ValueError(f'Data quality check failed for check #{i+1}: \
                        number of records:{num_records} \
                        NOT EQUAL TO expected:{exp_result}') 
                else:
                    self.log.info(f"Data quality check #{i+1} passed")
                    continue

            elif operator == 'ne':
                # not equal
                if not num_records != exp_result:
                    raise ValueError(f'Data quality check failed for check #{i+1}: \
                        number of records:{num_records} \
                        EQUAL TO expected:{exp_result}') 
                else:
                    self.log.info(f"Data quality check #{i+1} passed")
                    continue

            elif operator == 'lt':
                # smaller than
                if not num_records < exp_result:
                    raise ValueError(f'Data quality check failed for check #{i+1}: \
                        number of records:{num_records} \
                        NOT SMALLER THAN expected:{exp_result}') 
                else:
                    self.log.info(f"Data quality check #{i+1} passed")
                    continue

            elif operator == 'le':
                # smaller than or equal to
                if not num_records <= exp_result:
                    raise ValueError(f'Data quality check failed for check #{i+1}: \
                        number of records:{num_records} \
                        NOT SMALLER THAN OR EQUAL TO expected:{exp_result}') 
                else:
                    self.log.info(f"Data quality check #{i+1} passed")
                    continue        

            elif operator == 'gt':
                # greater than
                if not num_records > exp_result:
                    raise ValueError(f'Data quality check failed for check #{i+1}: \
                        number of records:{num_records} \
                        NOT GREATER THAN expected:{exp_result}') 
                else:
                    self.log.info(f"Data quality check #{i+1} passed")
                    continue   

            elif operator == 'ge':
                # greater than or equal to
                if not num_records >= exp_result:
                    raise ValueError(f'Data quality check failed for check #{i+1}: \
                        number of records:{num_records} \
                        NOT GREATER THAN OR EQUAL TO expected:{exp_result}') 
                else:
                    self.log.info(f"Data quality check #{i+1} passed")
                    continue   
            else:
                raise ValueError(f'Data quality invalid! check #{i+1} no operator like {operator}')


        self.log.info(f"Data quality checks passed")
        

                
