from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

import datetime
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 aws_conn_id,
                 redshift_conn_id,
                 s3_from,
                 s3_key,
                 table_to,
                 options,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_from = s3_from
        self.s3_key = s3_key
        self.table = table_to
        self.options = options
        self.autocommit = True
        self.region = 'us-west-2'

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        copy_options = '\n\t\t\t'.join(self.options)
        self.log.info('Connected to Redshift Database')

        copy_query = """
            COPY {table}
            FROM 's3://{s3_bucket}/{s3_key}'
            ACCESS_KEY_ID '',
            SECRET_ACCESS_KEY '',            
            {copy_options};
        """.format(table=self.table,
                   s3_bucket=self.s3_from,
                   s3_key=self.s3_key,

                   copy_options=copy_options)

        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY to database complete!")