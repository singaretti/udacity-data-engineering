from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 select_statement = '',
                 select_statement_append = '',
                 create_statement = '',
                 create_tmp_statement = '',
                 redshift_conn_id = '',
                 append_mode = '',
                 table_name = '',
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.select_statement = select_statement
        self.select_statement_append = select_statement_append
        self.create_statement = create_statement
        self.create_tmp_statement = create_tmp_statement
        self.table_name = table_name
        self.append_mode = append_mode

    def execute(self, context):
        
        self.log.info(f'Creating {self.table_name} dimension table if not exists')
        PostgresHook(postgres_conn_id=self.redshift_conn_id).run(self.create_statement)
        PostgresHook(postgres_conn_id=self.redshift_conn_id).run(self.create_tmp_statement)
        
        if self.append_mode:
            self.log.info(f"Append Mode ON: Inserting data from dimension temp tables to dimension table")
            insert_tmp_table = f"INSERT INTO {self.table_name}_tmp {self.select_statement_append}"
            PostgresHook(postgres_conn_id=self.redshift_conn_id).run(insert_tmp_table)
            
            insert_table = f"INSERT INTO {self.table_name} SELECT * FROM {self.table_name}_tmp"
            PostgresHook(postgres_conn_id=self.redshift_conn_id).run(insert_table)
            
            delete_table = f"DELETE FROM {self.table_name}_tmp"
            PostgresHook(postgres_conn_id=self.redshift_conn_id).run(delete_table)
        
        else:
            self.log.info(f"Append Mode OFF: Delete & Insert statements in {self.table_name} dimension table")
            PostgresHook(postgres_conn_id=self.redshift_conn_id).run(f"DELETE FROM {self.table_name};")
            PostgresHook(postgres_conn_id=self.redshift_conn_id).run(f"INSERT INTO {self.table_name} {self.select_statement}")