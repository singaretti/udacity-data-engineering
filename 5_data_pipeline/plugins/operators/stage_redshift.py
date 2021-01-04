from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id = '',
                 table_name = '',
                 s3_path = '',
                 redshift_conn_id = '',
                 copy_json_parameter = '',
                 drop_option = '',
                 *args,
                 **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_path = s3_path
        self.redshift_conn_id = redshift_conn_id
        self.copy_json_parameter = copy_json_parameter
        self.drop_option = drop_option

    def execute(self, context):
        
        aws_credentials = AwsHook(self.aws_credentials_id).get_credentials()

        if self.drop_option:
            self.log.info(f'Dropping {self.table_name} in {self.redshift_conn_id}')
            drop_statement = f"DROP TABLE IF EXISTS {self.table_name};"
            PostgresHook(postgres_conn_id=self.redshift_conn_id).run(drop_statement)
            
            create_table_command =  open(f'/home/workspace/airflow/plugins/helpers/create_{self.table_name}.sql', 'r').read()
            PostgresHook(postgres_conn_id=self.redshift_conn_id).run(create_table_command)
            
        self.log.info(f"Staging data from {self.s3_path} related to table {self.table_name} into {self.redshift_conn_id}")
        copy_command = "COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' FORMAT AS JSON '{}';"
        copy_command = copy_command.format(self.table_name,
                                           self.s3_path,
                                           aws_credentials.access_key,
                                           aws_credentials.secret_key,
                                           self.copy_json_parameter)
            
        self.log.info(f"Running {copy_command}")
        PostgresHook(postgres_conn_id=self.redshift_conn_id).run(copy_command)
        
        