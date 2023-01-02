'''
Operator for staging data from S3 to redshift
'''

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
        S3 to redshift copy operator
            Loads data from Amazon S3 to redshift stage.
        
        Args:
            redshift_conn_id: connection name in Airflow to Redshift
            aws_credentials_id: connection name in Airflow to Amazon
            table: table to copy from S3 to redshift
            s3_bucket: name of the S3 bucket
            s3_key: S3 path within bucket
            json_path: auto or path if specified
            region: AWS region - defaults to "us-west-2"
            file_format: file type defaults to "JSON"
            execution_date: date of execution defaults to None
            truncate: true/false - whether the table should be truncated first
    """  
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            JSON '{}' 
        """
    
    

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                aws_credentials_id='',
                table='',
                s3_bucket='',
                s3_key='',
                json_path="",
                region="us-west-2",
                file_format='JSON',
                execution_date = None,
                truncate = False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.file_format = file_format
        self.aws_credentials_id = aws_credentials_id
        self.json_path= json_path
        self.execution_date = execution_date
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        if self.truncate:
            self.log.info(f'Deleting table {self.table} from redshift')
            redshift.run(f'TRUNCATE {self.table}')

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        if self.execution_date:
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            s3_path = '/'.join([s3_path, str(year), str(month)])
            

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )

            
        redshift.run(formatted_sql)

        self.log.info(f"Copied {self.table} from S3 to Redshift stage.")



