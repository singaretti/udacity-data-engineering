from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 select_statement = '',
                 create_statement = '',
                 redshift_conn_id = '',
                 table_name = '',
                 *args,
                 **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.select_statement = select_statement
        self.create_statement = create_statement
        self.table_name = table_name

    def execute(self, context):
        
        self.log.info(f'Creating {self.table_name} fact table if not exists')
        PostgresHook(postgres_conn_id=self.redshift_conn_id).run(self.create_statement)
        
        self.log.info(f'Loading {self.table_name} fact table')
        PostgresHook(postgres_conn_id=self.redshift_conn_id).run(f"INSERT INTO {self.table_name} {self.select_statement}")
