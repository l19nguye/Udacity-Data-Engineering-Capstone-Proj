from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
        LoadDimensionOperator will be used to create a task which select data 
        from staginng tables then insert into dimension table.
        
        Will have a parameter:
            - ui_color: the color of task will displayed in Airflow Web server.
            
        
        Define class with following attributes:
            - insert_sql: the SELECT SQL command querying data from fact table.
            - redshift_conn_id: Redshift connection
    '''

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 insert_sql="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.insert_sql = insert_sql
        self.redshift_conn_id=redshift_conn_id
        
    def execute(self, context):
        '''
            - Retrieve and initialize Redshift connection
            - Redshift run the INSERT SQL command        
        '''
        
        self.log.info('LoadDimensionOperator is executing')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        
        redshift.run(self.insert_sql)
        
        self.log.info('LoadDimensionOperator finished.')