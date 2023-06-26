from datetime import datetime
from airflow.models import DAG,Variable
from airflow.operators.python import PythonOperator
import datetime  as dt
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime
import datetime  as dt


vault_url               = Variable.get("vault_url")
vault_token             = Variable.get("vault_token")
vault_meta_db           = Variable.get("vault_meta_db")  

default_arguments = {
    'owner': 'Airflow',
    'start_date': dt.datetime(2023, 3, 6)
}

schema_configuration_data = DAG('schema_configuration_data_load', default_args=default_arguments,
                     schedule_interval=dt.timedelta(minutes=20), catchup=False)

schema_configuration_data_loading = DatabricksSubmitRunOperator(
    task_id = 'schema_configuration_data_load',
    existing_cluster_id = '0328-130227-97kwwcng',
    databricks_conn_id = 'databricks_default',
    spark_python_task = { 
        'python_file': 's3://dataobservability/source/schema_configuration_data_load.py',
        'parameters':['-vaulturl',vault_url,'-vaulttoken',vault_token,'-vaultmetadb',vault_meta_db]
    },
    access_control_list = [
        {
            "user_name":"gutta.harikrishna@agilisium.com",
            "permission_level": "CAN_MANAGE"
        }
    ],
    dag = schema_configuration_data
)


schema_configuration_data_loading