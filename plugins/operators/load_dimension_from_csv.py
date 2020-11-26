from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionCsvOperator(BaseOperator):
    '''
        LoadDimensionCsvOperator will be used to create a task which select data 
        from staginng tables then insert into dimension table.
        
        Will have 3 parameters:
            - ui_color: the color of task will displayed in Airflow Web server.
            - insert_sql: the template of insert SQL command to adding data.
            
        
        Define class with following attributes:
            - table: is the dimension table name.
            - sql: the SELECT SQL command querying data from fact table.
            - redshift_conn_id: Redshift connection   
    '''

    ui_color = '#80BD9E'
    
    insert_sql = """
                    COPY  {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    csv
                    IGNOREHEADER 1
                    NULL 'NaN';
                """
    @apply_defaults
    def __init__(self,
                 table="",
                 s3_bucket="",
                 redshift_conn_id="",
                 aws_credential_id="",
                 *args, **kwargs):

        super(LoadDimensionCsvOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.s3_bucket = s3_bucket
        
    def execute(self, context):
        '''
            - Retrieve and initialize Redshift connection
            - Format INSERT SQL command with table name and SELECT query
            - If truncate flag is True, will need to add TRUNCATE SQL in front of the INSERT SQL
            - Redshift run the SQL command        
        '''
        
        self.log.info('LoadDimensionCsvOperator is executing dimension table: {}'.format(self.table))
        aws_hook = AwsHook(self.aws_credential_id)
        
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = LoadDimensionCsvOperator.insert_sql.format(
                            self.table,
                            self.s3_bucket, 
                            credentials.access_key,
                            credentials.secret_key                            
                        )
        
        
        redshift.run(formatted_sql)
        
        self.log.info('LoadDimensionCsvOperator finish dimension table: {}'.format(self.table))