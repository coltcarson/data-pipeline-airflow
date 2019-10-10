from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_json_to_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        COMPUPDATE OFF
    """    

    @apply_defaults
    def __init__(self,
                 aws_credentials_id = "",
                 redshift_conn_id = "",
                 table = "",
                 extra_params = "",
                 s3_key = "",
                 s3_bucket = "",
                 file_type = "",
                 json_path = "",
                 delimiter = ",",
                 ignore_headers = 1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.table = table
        self.extra_params = extra_params

    def execute(self, context):
        
        aws_web_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing Data From Target Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying Data From S3 Bucket Into Redshift")
        key = self.s3_key.format(**context)
        s3_bucket_path = "s3://{}/{}".format(self.s3_bucket, key)
        format_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            s3_bucket_path,
            self.extra_params
        )
        self.log.info(f"Executing {format_sql} ...")
        redshift.run(format_sql)    