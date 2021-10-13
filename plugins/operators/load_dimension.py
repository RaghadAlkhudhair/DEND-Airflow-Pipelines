from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    
    truncate_query = """
        TRUNCATE TABLE {};
    """
        
        
    insert_query = """
        INSERT INTO {}
        {};
    """
    
    
    
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 load_sql_stmt="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_stmt = load_sql_stmt
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            redshift.run(LoadDimensionOperator.truncate_query.format(self.table))
            
         To_Be_executed = LoadDimensionOperator.insert_query.format(
            self.table,
            self.load_sql_stmt
        )
        
        redshift.run(To_Be_executed)