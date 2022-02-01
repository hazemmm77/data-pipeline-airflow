from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
   

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql="",
                 table="",
                 flag="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table 
        self.sql=sql

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(flag=="truncate"):
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("loading data from staging tables to".format(self.table))
        
            
           
        
        redshift.run(self.sql)
        
        
