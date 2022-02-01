from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
   
    

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                
                  redshift_conn_id="",
                  aws_credentials_id="",
                  dq_checks="",
                  *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.dq_checks=dq_checks
        
       

    def execute(self, context):
      
        i
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for index in range(len(self.dq_checks)):
            for key in self.dq_checks[index]:
                records = redshift.get_records(key.get('check_sql'))
                expected_result= key.get('expected_result')                              
                if (records[0]!=expected_result):
                    raise ValueError("Data quality check failed")
        
       
        