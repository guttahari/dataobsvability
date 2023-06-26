from datetime import datetime
from airflow import DAG
from teams_alerts import *
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator
)
from datetime import datetime
import datetime  as dt



# Replace these with your correct values
JOB_ROLE_ARN = "arn:aws:iam::919490798061:role/AmazonEMR-ExecutionRole-1678097708843"
S3_LOGS_BUCKET = "s3://dataobservability/logs/"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
    "s3MonitoringConfiguration": {"logUri": "s3://dataobservability/logs/"}
    },
}

with DAG(
    dag_id="schema-configuration-data-load",
    schedule_interval="0 * * * *",
    start_date=datetime(2023, 2, 1),
    tags=["example"],
    catchup=False,
    max_active_runs=1
) as dag:



    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_app",
        job_type="SPARK",
        release_label="emr-6.10.0",
        config={"name": "airflow-emr-schema-configration-data-load","imageConfiguration": {"imageUri": "919490798061.dkr.ecr.us-west-2.amazonaws.com/emr-serverless-ci:latest"},
                "networkConfiguration":{"subnetIds": ["subnet-0f7f1221c24efa7bc"], "securityGroupIds": ["sg-0e84865e11fe96c10"]}, 
                "initialCapacity": {"DRIVER": {"workerCount": 1,"workerConfiguration": {"cpu": "2vCPU","memory": "4GB"}},
                "EXECUTOR": {"workerCount": 2,"workerConfiguration": {"cpu": "2vCPU","memory": "4GB"}}}},
        aws_conn_id="aws_region" 
    )

    application_id = create_app.output

    job1 = EmrServerlessStartJobOperator(
        task_id="start_job_1",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        aws_conn_id="aws_region",
        job_driver={
            "sparkSubmit": {
                "entryPoint": 's3://dataobservability/source/schema_configuration_data_load.py',
                "sparkSubmitParameters": "--jars s3://dataobservability/jars/sqljdbc4-2.0.jar,s3://dataobservability/jars/spark-sql_2.13-3.3.2.jar,s3://dataobservability/jars/postgresql-42.6.0.jar,s3://dataobservability/jars/DatabricksJDBC42.jar,s3://dataobservability/jars/redshift-jdbc42-2.1.0.12.jar",
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    create_app >> job1 