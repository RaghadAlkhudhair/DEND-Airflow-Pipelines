from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS '{}'
        ;
    """
    
    
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 formatting="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.region=region
        self.formatting=formatting

    def execute(self, context):
          aws_hook = AwsHook(self.aws_credentials_id)
          credentials = aws_hook.get_credentials()
          redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
          self.log.info(f"Extracing data from S3 to Redshift staging for the table '{self.table}'")
          rendered_key = self.s3_key.format(**context)
          s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
            
          Query_To_Execute = StageToRedshiftOperator.query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.formatting)
        
        
          self.log.info(f"Executing query to extract data from '{s3_path}' to '{self.table}'")
          redshift.run(Query_To_Execute)





