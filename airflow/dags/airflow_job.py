from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)
from helpers import SqlQueries

# Set up the default arguments for airflow

default_args = {
    'depends_on_past': False,
    'owner': 'Ashfaqur Rahman',
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
    'start_date': datetime(2016, 1, 1),
    'email_on_retry': False,
}

# Create the DAG
dag = DAG('capstone_project',
          default_args=default_args,
          description='Load data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin',  dag=dag)

immigration_to_redshift = StageToRedshiftOperator(
    task_id='Immigration_Fact_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'udacity-capstone',
    s3_key = 'immigration_data.parquet',
    table = 'immigration',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

world_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Country_Dimension_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'udacity-capstone',
    s3_key = 'world_temperature_data.parquet',
    table = 'world_temperature',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

usdemographic_to_redshift = StageToRedshiftOperator(
    task_id='State_Dimension_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'udacity-capstone',
    s3_key = 'us_demographic_data.parquet',
    table = 'usdemographic_info',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Data_Quality_Checks',
    redshift_conn_id = "redshift",
    tables=['immigration', 'world_temperature', 'usdemographic_info'],
    dag=dag
)

end_operator = DummyOperator(task_id='End',  dag=dag)

# Set up the task dependencies
start_operator >> immigration_to_redshift
immigration_to_redshift >> world_temperature_to_redshift
immigration_to_redshift >> usdemographic_to_redshift
immigration_to_redshift >> run_quality_checks
world_temperature_to_redshift >> run_quality_checks
usdemographic_to_redshift >> run_quality_checks
run_quality_checks >> end_operator