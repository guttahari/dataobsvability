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

vault_url               = Variable.get("vault_url")
vault_token             = Variable.get("vault_token")
vault_meta_db           = Variable.get("vault_meta_db")  


Environment = Variable.get("Environment")
validation = Variable.get("schema_anamoly_endpoint")
execution_date='{{ next_ds }}'

queries =   [
                    {
                        'query': f"""SELECT source_name as Data_Source,column_name as Attributes,{validation} AS validation FROM missing_columns_view_{Environment} main where date(modified_date)='{execution_date}' order by 1,2""",
                        'text_name': 'Missing Attributes'
                    },
                    {
                        'query': f"""SELECT source_name as Data_Source,column_name as Attributes,{validation} AS validation  FROM new_columns_view_{Environment} main where date(modified_date)='{execution_date}' order by 1,2""",
                        'text_name': 'New Attributes'
                    },
                    {
                        'query': f"""SELECT source_name as Data_Source,column_name as Attributes,data_type_old as Previous,data_type as Current,{validation} AS validation  FROM data_type_change_view_{Environment} main where date(modified_date)='{execution_date}' order by 1,2""",
                        'text_name': 'Datatype Changes'
                    },
                    {
                        'query': f"""SELECT source_name as Data_Source,column_name as Attributes,character_maximum_length_old as Previous,character_maximum_length as Current,{validation} AS validation  FROM length_changes_view_{Environment} main where date(modified_date)='{execution_date}' order by 1,2""",
                        'text_name': 'Length Changes'
                    },
                    {  
                        'query': f"""SELECT source_name as Data_Source,column_name as Attributes,ordinal_position_old as Previous,ordinal_position as Current,{validation} AS validation  FROM ordinal_change_view_{Environment} main where date(modified_date)='{execution_date}' order by 1,2""",
                        'text_name': 'Ordinal Position'
                    },
                    {
                        'query': f"""SELECT source_name as Data_Source,column_name as Attributes,is_nullable_old as Previous,is_nullable as Current,{validation} AS validation  FROM nullable_change_view_{Environment} main where date(modified_date)='{execution_date}' order by 1,2""",
                        'text_name': 'Nullable Changes'
                    },
                    {
                        'query': f"""SELECT source_name as Data_Source,{validation} AS validation  FROM table_deleted_view_{Environment} main where date(modified_date)='{execution_date}' order by 1""",
                        'text_name': 'Missing Data Source'
                    }
                ]

replace_view_queries = [
                            {
                                'query':'select true'
                            }
                            ]
# Replace these with your correct values
JOB_ROLE_ARN = "arn:aws:iam::919490798061:role/AmazonEMR-ExecutionRole-1678097708843"
S3_LOGS_BUCKET = "s3://dataobservability/logs/"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
    "s3MonitoringConfiguration": {"logUri": "s3://dataobservability/logs/"}
    },
}

with DAG(
    dag_id="schema-anamoly-detection",
    schedule_interval="0 7 * * *",
    start_date=datetime(2023, 2, 1),
    tags=["example"],
    catchup=False,
    max_active_runs=1
) as dag:

    notification_alert = PythonOperator(
        task_id='notification_alert',
        provide_context=True,
        python_callable=alert,
        op_kwargs={'meta_db_cred': Variable.get("meta_db_cred"),
                "text": f"Schema Anomalies Detected for '{execution_date}'",
                'link':'https://us-east-1.quicksight.aws.amazon.com/sn/dashboards/84bd54fe-fac5-4150-8d47-dc7f913276c5',
                'queries': queries,
                'replace_view_queries' : replace_view_queries
        })
    


    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_app",
        job_type="SPARK",
        release_label="emr-6.10.0",
        config={"name": "airflow-emr-schema-anamoly-detection","imageConfiguration": {"imageUri": "919490798061.dkr.ecr.us-west-2.amazonaws.com/emr-serverless-ci:latest"},
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
                "entryPoint": "s3://dataobservability/source/schema_anamoly_detection.py",
                "sparkSubmitParameters": "--jars s3://dataobservability/jars/sqljdbc4-2.0.jar,s3://dataobservability/jars/spark-sql_2.13-3.3.2.jar,s3://dataobservability/jars/postgresql-42.6.0.jar,s3://dataobservability/jars/DatabricksJDBC42.jar,s3://dataobservability/jars/redshift-jdbc42-2.1.0.12.jar",
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    create_app >> job1 >> notification_alert