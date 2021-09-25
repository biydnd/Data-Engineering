from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 insert_dim_sql="",
                 append_mode=None,
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id=redshift_conn_id
        self.insert_dim_sql=insert_dim_sql
        self.append_mode=append_mode
        self.table=table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_mode == False:
            redshift.run( "DELETE FROM TABLE {}".format(self.table) ) 
        self.log.info('Load dimension table')
        redshift.run(self.insert_dim_sql)
