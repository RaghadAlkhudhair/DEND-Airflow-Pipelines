from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
          query_check="",
          redshift_conn_id="",
          *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_check=query_check

    def execute(self, context):
       redshift_hook = PostgresHook(self.redshift_conn_id)         
       query=self.query_check.get('check_sql')
       expected_result=self.query_check.get('expected')
            
            
       try:
         self.log.info(f"Running query: {query}")
         records = redshift_hook.get_records(query)[0]
       except Exception as e:
         self.log.info(f"Query failed with exception: {e}")
                
       if exp_result != records[0]:
          self.log.info('DQ Validation Failed')
          raise ValueError('DQ Validation Failed')
                    
       else:
          self.log.info("DQ Validation Succeeded")