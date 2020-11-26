from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import airflow.macros as macros

class StageToRedshiftOperator(BaseOperator):
    '''
        StageToRedshiftOperator will be used to create a task to loading data 
        from JSON files locating in a S3 to a staging table in Redshift.
        
        Will have 3 parameters:
        - ui_color: the color of task will be displayed in Airflow Web server.
        - template_fields: the list of fields will take the values from context.
        - copy_sql: the template of COPY SQL command to copying JSON data into staging table.
        
        In order to do that, we will define the class with following attributes.
        
        - redshift_conn_id: the connection to Redshift cluster.
        - table: the staging table name.
        - s3_bucket: the S3 URL where JSON files locating.
        - iam_role: the IAM role using to loading data into Redshift.
        - execution_date: the date when the task executed in format YYYY-MM-DD
        - json_suffix: which is suffix '-events.json' of each event JSON file. 
 
    '''
    ui_color = '#358140'
    
    copy_sql = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    JSON 'auto ignorecase';
                """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table="",
                 s3_bucket="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.s3_bucket = s3_bucket

    def execute(self, context):
        """
            First, initialize the connection to Reshift cluster.
            Before loading data, we execute delete SQL in Redshift to clear all existing data.
            Build the command will be used to coping data from JSON files in S3 to Redshift table.
            Redshift run the command to start copying data.
        """
        
        self.log.info('StageToRedshiftOperator is executing staging table {}'.format(self.table))
        
        aws_hook = AwsHook(self.aws_credential_id)
        
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 

        self.log.info("StageToRedshiftOperator is clearing data from staging table {}".format(self.table))
        
        redshift.run("DELETE FROM {}".format(self.table))        
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                            self.table,
                            self.s3_bucket, 
                            credentials.access_key,
                            credentials.secret_key                                                       
                        )
        
        self.log.info("StageToRedshiftOperator is copying data from S3 to staging table {}".format(self.table))
        
        redshift.run(formatted_sql)
        
        self.log.info('StageToRedshiftOperator finishes staging table {}'.format(self.table))