from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''
        LoadFactOperator will be used to create a task which select data 
        from staging tables then insert into fact table.
        
        Will have parameter:
            - ui_color: the color of task will displayed in Airflow Web server.        
        
        Define class with following attributes:
            - insert_sql: the INSERT SQL command by querying data from staging table.
            - redshift_conn_id: Redshift connection   
    '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 insert_sql="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.insert_sql = insert_sql
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        
        self.log.info('LoadFactOperator is executing.')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        redshift.run(self.insert_sql)
        
        self.log.info('LoadFactOperator finished.')