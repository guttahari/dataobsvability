from airflow.models import DAG,Variable
from airflow.operators.python import PythonOperator
import datetime  as dt
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from teams_alerts import *
from airflow.models import DAG, Variable
from datetime import date


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
      


default_arguments = {
    'owner': 'Airflow',
    'start_date': dt.datetime(2023, 3, 6)
}

schema_anamoly_detection = DAG('schema_anamoly_detection', default_args=default_arguments,
                     schedule_interval="0 7 * * *", catchup=False)


schema_anamoly_loading = DatabricksSubmitRunOperator(
    task_id = 'schema_difference_loading', 
    existing_cluster_id = '0328-130227-97kwwcng',
    databricks_conn_id = 'databricks_default',
    spark_python_task = { 
        'python_file': "s3://dataobservability/source/schema_anamoly_detection.py",
        'parameters':['-vaulturl',vault_url,'-vaulttoken',vault_token,'-vaultmetadb',vault_meta_db]
    },
  
    access_control_list = [
        {
            "user_name":"gutta.harikrishna@agilisium.com",
            "permission_level": "CAN_MANAGE"
        }
    ],
    dag = schema_anamoly_detection)


notification_alert = PythonOperator(
    task_id='notification_alert',
    provide_context=True,
    python_callable=alert,
    op_kwargs={'meta_db_cred': Variable.get("meta_db_cred"),
               "text": f"Schema Anomalies Detected for '{execution_date}'",
               'link':'https://us-east-1.quicksight.aws.amazon.com/sn/dashboards/84bd54fe-fac5-4150-8d47-dc7f913276c5',
               'queries': queries,
               'replace_view_queries' : replace_view_queries
    },
    dag=schema_anamoly_detection
)

schema_anamoly_loading >> notification_alert