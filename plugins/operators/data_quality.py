from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
        DataQualityOperator will be used to create a task which will count the 
        invalid records of both fact and dimension tables.
        
        If any tables has at least one invalid records, will raise a value error,
        and the task will be failed.
        
        Otherwise, the task will be completed successfully.
        
        
        Will have 2 parameters:
            - ui_color: the color of task will displayed in Airflow Web server.
            - sql_check: the dictionary consist SQL commands using to count invalid records of each table and the result expected.            
        
        Define class with following attributes:
            - redshift_conn_id: Redshift connection   
    '''

    ui_color = '#89DA59'
    
    sql_check = {
        'song': { "sql": " SELECT COUNT(1) FROM public.artists WHERE artistid IS NULL ", "expect_result": 0 },
        'artists': { "sql": " SELECT COUNT(1) FROM public.songs WHERE songid IS NULL ", "expect_result": 0 },
        'songplays': { "sql": """ SELECT COUNT(1) FROM public.songplays WHERE playid IS NULL 
                                                                      OR start_time IS NULL 
                                                                      OR userid IS NULL """, "expect_result": 0 },
        'time': { "sql": " SELECT COUNT(1) FROM public.time WHERE start_time IS NULL ", "expect_result": 0 },
        'users': { "sql": " SELECT COUNT(1) FROM public.users WHERE userid IS NULL ", "expect_result": 0 }
    }
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
            First, retrieve and initialize Redshift connection.
            Then, iterate over each item of dictionary sql_check,
            run the SELECT SQL for each table, then compare the 
            returned result to the expected one.
            
            If any one got failed, we will write informational log, 
            then raise ValueError
            
            Otherwise, will write an informational log to inform 
            the data of tables are valid and finish the check.
        """
        
        self.log.info('Start validating data.')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table, check in DataQualityOperator.sql_check.items():
            sql = check['sql']
            
            expect_result = check['expect_result']
            
            records = redshift.get_records(sql)[0]
        
            if records[0] != expect_result:
                self.log.info(f"Table {table} did not pass the test." )
                self.log.info(sql)
                raise ValueError("Data quality check failed")
        
        self.log.info('All tables have valid data.')
        
        self.log.info('Finished validating data.')