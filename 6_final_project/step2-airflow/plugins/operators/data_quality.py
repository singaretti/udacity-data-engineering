from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,redshift_conn_id = '',
                 tables_in_data_quality_task = '',
                 columns_in_data_quality_task = '',
                 number_of_rows_to_check = '',
                 *args,
                 **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_in_data_quality_task = tables_in_data_quality_task
        self.columns_in_data_quality_task = columns_in_data_quality_task
        self.number_of_rows_to_check = number_of_rows_to_check

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        magic_number = self.number_of_rows_to_check
        
        for table, column in zip(self.tables_in_data_quality_task, self.columns_in_data_quality_task):
            self.log.info(f"Data Quality -----> count(*) in {table} <-----")
            
            count = redshift.get_records(f"select count(*) from {table};")
            count = count[0][0]
            if count < magic_number:
                raise ValueError(f"Data quality COUNT check failed: {table}:{column}, count(*) = {count}")
            else:
                print(f"Table {table}, Column {column}, count(*) = {count}")
            null_column = redshift.get_records(f"select count(*) from {table} where {column} = null;")
            null_column = null_column[0][0]
            if null_column > magic_number:
               raise ValueError(f"Data quality NULL COUNT check failed: {table}:{column}, null count(*) = {null_column}")
            else:
                print(f"Table {table}, Column {column}, null count(*) = {null_column}, magic number = {magic_number}")