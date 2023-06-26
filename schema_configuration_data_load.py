# Description :

# In This file we are loading the schema information data of different tables stored in the different connectors and loading them into the schema_configuration table based up on the new entries in the master_configuration table
 

# Process Overview 
# Step-1 : Fetch the latest last_run_date from master_config_status table 
# Step-2 : Identify the new entries in master_configuration whose check_type is 'schema' and greater than last_run_date
# Step-3 : Connect the vault to retrieve source db credentials.
# Step-4 : For Each entry in master_configuration Fetch the schema details from the respective connectors and store it in schema_configuration
# Step-5 : Update the last_run_date as latest updated_at from master configuraton

# Input and Output : 
# Input will be the entries from master configuration where check_type is schema in UI 

# Output Data will be stored in the schema_configration with the following fields: 
# - config_id
# - column_name
# - data_type
# - ordinal_position 
# - is_nullable
# - character_maximum_length changes data 


# Tables Affected : 
# - master_configuration || Read            #contains all the mete information about the tables

# sample data:
# config_id  Table_name                          check_type     created_by      is_active  is_deleted       updated_at
# -------------------------------------------------------------------------------------------------------------------------------------------------
# 463	     connector:catalog.schema.table1	 schema			axz@vfd.com		true	   false			2023-04-17 14:12:40.172 +0530	
# 464	     connector:catalog.schema.table2	 schema			axz@vfd.com		true	   false			2023-04-17 14:19:53.768 +0530	
# 2023-04-18 11:55:24.711	



# - Schema_configration || Read,Write       #information of all columns for each table
# sample_data:
# column_name   data_type           is_nullable    character_maximum_length     ordinal_position   config_id
# -------------------------------------------------------------------------------------------------------------------------------------------------
# country_code	character varying	YES	           7	                        5	               463
# id	        integer	            yes	           0	                        1	               463

# - master_config_status || Read,Write      # Last run time of the Job
# sample_data:
# check_type        updated_at
# schema	    2023-04-19 10:19:15.083
# distribution	2023-04-18 12:05:05.670

# Import necessary libraries/modules
import io
import logging
import boto3
import psycopg2
import hvac
import argparse
from psycopg2 import Error
from datetime import datetime
from py4j.protocol import Py4JJavaError
from psycopg2 import Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import *
import pandas as pd


# url='http://54.186.22.112:8200'
# token="hvs.sdv1d3xW9G2vdGohes1BnJUt"
# vault_meta_db = "db_postgresql_dev"

# Importing variables from airflow
parser = argparse.ArgumentParser()
parser.add_argument('-vaulturl')
parser.add_argument('-vaulttoken')
parser.add_argument('-vaultmetadb')

args = parser.parse_args()

url                     =   args.vaulturl
token                   =   args.vaulttoken
vault_meta_db           =   args.vaultmetadb

# Get credentials for each role in the vault
def write_logs(body, bucket, key):
    s3 = boto3.client("s3")
    s3.put_object(Body=body, Bucket=bucket, Key=key)
    
def connection_to_vault(url,token,role):
    try:
      client = hvac.Client(url=url,token=token)
      result_key = client.read(role)
    except hvac.exceptions as e:
      print(e.java_exception.getMessage())
    
    return result_key
  
def list_secrets(client, path=None, mount_path=None):
    list_response = client.secrets.kv.v2.list_secrets(
        path=path,
        mount_point=mount_path
    )

    return list_response

def read_from_vault(client,role):

    #client = hvac.Client(url=url,token=token)
    result_key = client.read(role)
    
    return result_key

def get_all_db_credentials(url,token):

    # Set the Vault address and token
    client = hvac.Client(
            url=url,  # Replace with your Vault URL
            token=token  # Replace with your Vault token
            )
    secret_list = list_secrets(client,path='',mount_path='kv')
    secret_key_list = secret_list['data']['keys']
    print(secret_key_list)
    role_list = []
    credentials = {}
    for row in secret_key_list:
      try:
        if 'db_' in row and '_dev' in row:
          role_path = 'kv/data/'
          role = role_path + row
          #role_list.append(role)
          database={}
          
          cred = read_from_vault(client,role)
          #database.update(cred['data']['data'])
          database[row] = cred['data']['data']
          conn_string =  "jdbc:{connector}://{host}:{port}/{database}".format(connector=database[row]['connector'].lower(),host=database[row]['host'],port=database[row]['port'],database=database[row]['database'])
          if database[row]['connector'].lower() == 'postgresql':
              driver = 'org.postgresql.Driver'
          elif database[row]['connector'].lower() == 'redshift':
              driver = 'com.amazon.redshift.jdbc42.Driver'
          elif database[row]['connector'].lower() == 'snowflake':
              driver = 'net.snowflake.client.jdbc.SnowflakeDriver'
          elif database[row]['connector'].lower() == 'deltalake':
              driver = None
          database[row].update({"conn_string":conn_string})
          database[row].update({"driver":driver})
          credentials.update(database)
      except Exception as e:
        log.error('Exception occurred: %s', e)
        display("Error occured for secret {secret_name}".format(secret_name=row))
    return credentials

  
spark = SparkSession.builder.appName("schema_configuration_data_load").getOrCreate()
current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
bucket_name = 'dataobservability'
key ='schema/logs/output_{current_timestamp}.log'.format(current_timestamp=current_timestamp)

# Step 3 : Connect the vault to retrieve source db credentials.
credentials = get_all_db_credentials(url,token)

meta_user = credentials[vault_meta_db]['username']
meta_password = credentials[vault_meta_db]['password']
meta_driver = credentials[vault_meta_db]['driver']
meta_conn_string = credentials[vault_meta_db]['conn_string']
meta_db = credentials[vault_meta_db]['database']
meta_port = credentials[vault_meta_db]['port']
meta_host = credentials[vault_meta_db]['host']

# key functions
# This function inserts a DataFrame into a specified table in a database using JDBC driver
def insert_df_into_tbl(df,destination_tbl,conn_string):
    
    df.write.format("jdbc").mode("append").option("url",conn_string).option("driver", meta_driver).option("dbtable",f"{destination_tbl}") .option("user",meta_user).option("password",meta_password).save()
    
    display("Data Loaded")
    
# This function that reads data from a database table or view into a Pandas DataFrame using Apache Spark or SQL, depending on the database name. The resulting Pandas DataFrame is then returned from the function.
def read_tbl_into_df(query,db_name,conn_string,user,password,driver,spark):
    # read the data from the database using the JDBC format
    #spark = SparkSession.builder.appName("Schema").getOrCreate()
    if db_name != 'Deltalake':
      data_frame = (spark.read.format("jdbc").option("driver", driver)
        .option("url", conn_string)
        .option("query",query)
        .option("user", user)
        .option("password", password)
        .load())
    else:
      data_frame = spark.sql(query)
    return data_frame.toPandas()
  
#  This function that executes a SQL query on a PostgreSQL database using psycopg2 library and closes the database connection after the query is executed.
def execute_sql(sql,user,password,host,port,db):
    try:
        connection = psycopg2.connect(user=user,password=password,host=host,port=port,database=db)
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# loading the data into the schema_configration
def load_data_into_table(df,meta_conn_string):
  df_spark = spark.createDataFrame(df)
  insert_df_into_tbl(df_spark,'schema_configuration',meta_conn_string)
          
# This function is used to extract the information from different connectors(i.e redshift,snowflake) 
def fetching_information_schema_from_source(config_id,source,catalog_name,schema,table,user,password,driver,source_conn_string):
    fetching_schema = '''SELECT lower(column_name) AS "column_name", 
                            lower(data_type) AS "data_type",
                            lower(is_nullable) AS "is_nullable",
                            (case when character_maximum_length  is null then 0 else character_maximum_length end) as "character_maximum_length",
                            lower(ordinal_position)::integer AS "ordinal_position"
                            FROM {}.information_schema.columns
                            WHERE table_catalog = '{}'
                            and table_schema ='{}'
                            AND table_name = '{}' '''
    fetching_schema_query = fetching_schema.format(catalog_name,catalog_name, schema, table)
    result = read_tbl_into_df(fetching_schema_query, catalog_name,source_conn_string,user,password,driver,spark)
    return result
  
# Loading the fetching_information_schema_from_source data into the destination table 
def loading_data_into_schema_configuration(config_id,source,catalog_name,schema,table,user,password,driver,source_conn_string,meta_conn_string):  
#   fetching the data
    log.info("Passing parameters to fetching_information_schema_from_source function for config_id = {config_id} !!!".format(config_id=config_id))
    result = fetching_information_schema_from_source(config_id,source,catalog_name,schema,table,user,password,driver,source_conn_string)
    print(result)
    if len(result) > 0:
        result['config_id'] = config_id 
#       loading the data
        log.info("loading the data by load_data_into_table function for config_id = {config_id} !!!".format(config_id=config_id))
        load_data_into_table(result,meta_conn_string)
        print(load_data_into_table)
    else:
        print("no respective table in source")

          
def schema_configuration_data_load(url,token,vault_meta_db):

    try:
        # Step-1
        # Identify last_run_date from master_config_status
        query = '''SELECT updated_at as last_run_date from master_config_status where check_type = 'schema' '''
        query_result = read_tbl_into_df(query,"meta_data",meta_conn_string,meta_user,meta_password,meta_driver,spark)
        last_run_date = query_result['last_run_date'].iloc[0]

        #Step-2
        # Identify the new entries in master_configuration whose check_type is 'schema' and gretaher than last_run_date
        master_config_query = """SELECT config_id,
                                lower(split_part(table_name, ':', 1)) AS source,
                                split_part(split_part(table_name, ':', 2), '.', 1) AS catalog_name,
                                split_part(split_part(table_name, ':', 2), '.', 2) AS schema_name,
                                split_part(split_part(table_name, ':', 2), '.', 3) AS tbl_name,
                                is_deleted::text
                                FROM 
                                    master_configuration
                                WHERE 
                                    check_type = 'schema' and "updatedAt" >  '{last_run_date}' and config_id
                                not in (SELECT DISTINCT config_id FROM  schema_configuration)  """.format(last_run_date = last_run_date)        
        master_configuration_result_df = read_tbl_into_df(master_config_query, 'meta_data',meta_conn_string,meta_user,meta_password,meta_driver,spark)

        # Step-4 : For Each entry in master_configuration Fetch the schema details from the respective connectors and store it in schema_configuration
        # Proceed if new entries is available in the master configuration table 
        if not master_configuration_result_df.empty :
            
            log.info("Passing parameters to loading_data_into_schema_configuration function ")

            master_configuration_result_df.apply(lambda row: loading_data_into_schema_configuration(row['config_id'],row['source'],row['catalog_name'], row['schema_name'], row['tbl_name'],credentials['db_'+row['source']]['username'],credentials['db_'+row['source']]['password'],credentials['db_'+row['source']]['driver'], credentials['db_'+row['source']]['conn_string'],meta_conn_string), axis=1)
            

        # Step-5 : Update the last_run_date as latest updated_at from master configuraton to master_config_status table
        update_status = '''UPDATE master_config_status
                                set updated_at = (select max("updatedAt") from master_configuration where check_type = 'schema')
                                where check_type = 'schema' '''
        execute_sql(update_status,meta_user,meta_password,meta_host,meta_port,meta_db)
        
    except Exception as e:
        log.error('Exception occurred: %s', e)
        print("Error:" + str(e))

    


if __name__ == "__main__":
  log = logging.getLogger("schema_log")
  log.setLevel(logging.INFO)
  formatter = logging.Formatter('%(asctime)s,schema,%(levelname)s,%(funcName)s,%(message)s')

  log_stringio = io.StringIO()
  handler = logging.StreamHandler(log_stringio)
  handler.setFormatter(formatter)
  log.addHandler(handler)
  schema_configuration_data_load(url,token,vault_meta_db)
  write_logs(body=log_stringio.getvalue(), bucket=bucket_name, key=key)
      