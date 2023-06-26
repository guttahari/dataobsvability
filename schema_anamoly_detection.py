# Description :

# This python file helps us to detect the anomalies between the previous schema to present schema.

# Process Overview 
# Step-1 : Fetch all the entries from master_configuration where check_type is schema
# Step-2 : Connect the vault to retrieve source db credentials.
# Step-3 : Fetch stored schema details from schema_configuration based on the table_name into a exist_schema_result_df
# Step-4 : Fetch current schema details from source and store into a current_schema_result_df
# Step-5 : Compare that two df's and store that anamolies into a compare_schema_result_df
# Step-6 : Load the anamoly result into the schema_anamoly



# Input and Output : 
# Input will be the all the existing data from schema configuration  and current schema from differenet sources like redshift and snowflake

# Output Data will be stored in the schema_anamoly with the following fields: 
# - config_id
# - column_name
# - data_type,
# - data_type_old,
# - ordinal_position ,
# - ordinal_position_old,
# - is_nullable,
# - is_nullable_old,
# - character_maximum_length,
# - character_maximum_length_old,
# - id,
# - anamoly_id,
# - updated_at,
# - user_input,
# - anamoly


# Tables Affected : 
# - master_configuration || Read

# sample data:
# config_id  Table_name                          check_type     created_by          is_active  is_deleted    updated_at   
# 463	     connector:catalog.schema.table1	 schema			axz@vfd.com			true	   false	     2023-04-18 11:55:24.711
# 464	     connector:catalog.schema.table2	 schema			axz@vfd.com	     	true	   false		 2023-04-17 14:19:53.768 +0530	


# - Schema_anamoly || Read,Write
# sample_data:

# id.   anamoly_id  updated_at                      column_name     data_type_old   data_type                   user_input   anamoly  config_id
# 24731	   2	    "2023-04-19 10:37:24.063481"	"id"	        "number"	    "double precision"			false	     true	  471
# 24732	   2	    "2023-04-19 10:37:24.063481"	"country_code"	"text"	        "varchar"					false	     true	  471

# Import necessary libraries/modules
import io
import logging
import boto3
import psycopg2
from psycopg2 import Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import *
import hvac
import pandas as pd
import argparse

# Step-1
# Importing variables from airflow
parser = argparse.ArgumentParser()
parser.add_argument('-vaulturl')
parser.add_argument('-vaulttoken')
parser.add_argument('-vaultmetadb')

args = parser.parse_args()

url                     =   args.vaulturl
token                   =   args.vaulttoken
vault_meta_db           =   args.vaultmetadb

# Step-1
# Setting up the Vault URL and token
# url='http://54.186.22.112:8200'
# token="hvs.sdv1d3xW9G2vdGohes1BnJUt"
# vault_meta_db = "db_postgresql_dev"

# Get List of all  database roles stored in the Vault
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

  
spark = SparkSession.builder.appName("schema_anamoly_detection").getOrCreate()

#   Step-2 : Connect the vault to retrieve source db credentials.
credentials = get_all_db_credentials(url,token)            
currentDateAndTime = datetime.now()
# currentDateAndTime = currentDateAndTime - timedelta(days=1)
print(currentDateAndTime.date())
extra_columns = ['anamoly_id', 'updated_at']
bucket_name = 'dataobservability'
key ='schema/logs/output_{current_timestamp}.log'.format(current_timestamp=currentDateAndTime)


meta_user = credentials[vault_meta_db]['username']
meta_password = credentials[vault_meta_db]['password']
meta_driver = credentials[vault_meta_db]['driver']
meta_conn_string = credentials[vault_meta_db]['conn_string']
meta_db = credentials[vault_meta_db]['database']
meta_port = credentials[vault_meta_db]['port']
meta_host = credentials[vault_meta_db]['host']


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

# Step-4
def comparing_both_dataframes(exist_schema_result_df,current_schema_result_df):
    anamolies_list = "select distinct anamoly,compare_columns,anamoly_type from anamoly_master "
    anamolies_list_df = read_tbl_into_df(anamolies_list, 'meta_data',meta_conn_string,meta_user,meta_password,meta_driver,spark)
    
    result_list = []
    for index, row in anamolies_list_df.iterrows():
        anamoly = row['anamoly']
        compare_columns = row['compare_columns']

        #compare_columns Data should be like [table_name, column_name, data_type]
        anamoly_type = row['anamoly_type']
        # anamoly_type data should be like [1,2,3,4,5,..]

        current_schema_df_relevant = current_schema_result_df[compare_columns]
        # current_schema_df_relevant data will be look like[table_name, column_name, data_type]
        exist_schema_df_relevant = exist_schema_result_df[compare_columns]
        # exist_schema_df_relevant data will be look like[table_name, column_name, data_type]

        if anamoly in ["datatype", "length", "is_null", "ordinal"]:

            last_column = compare_columns[-1]
            exist_schema_df_relevant = exist_schema_df_relevant.rename(
                    columns={last_column: last_column+ "_old"})

            df_merged = pd.merge(
                exist_schema_df_relevant,
                current_schema_df_relevant,
                how="left",
                on=["table_name", "column_name"],
            ).assign(
                anamoly_id=anamoly_type,
                updated_at=currentDateAndTime
            )    
            selected_cols_with_data = df_merged[(df_merged[last_column + "_old"] != df_merged[last_column])]
            result_list.append(selected_cols_with_data)
        
        else:
            # Similar way for all the remaining data types
            if anamoly in ["new_column"]:
                merge_condition = "right_only"
            else:
                merge_condition = "left_only"
            if anamoly in ['table_miss']:
                on_condition = ["table_name"]
            else:
                on_condition = ["table_name", "column_name"]
            df_merged = pd.merge(
                exist_schema_df_relevant,
                current_schema_df_relevant,
                how="outer",
                indicator=True,
                on=on_condition
            ).assign(
                anamoly_id=anamoly_type,
                updated_at=currentDateAndTime
            )
            df_merged = df_merged[(df_merged._merge == merge_condition)]
            selected_cols_with_data = df_merged.loc[:, extra_columns + on_condition]  
            result_list.append(selected_cols_with_data)
        
    return pd.concat(result_list, ignore_index=True)

       
# fetching the stored schema details from schema_configuration data into the exist_schema_df
def fetch_stored_schema_details_from_schema_configuration(table_name,meta_conn_string,meta_user,meta_password,meta_driver,spark):

    exist_data_query = """select lower(table_name) as table_name,column_name, 
                data_type,
                ordinal_position,
                is_nullable,
                (case when character_maximum_length  is null then 0 else character_maximum_length end) as "character_maximum_length"
                from schema_configuration_view sc  where sc.table_name like '{}' """
    exist_data = exist_data_query.format(table_name)
    exist_schema_df = read_tbl_into_df(exist_data, 'meta_data',meta_conn_string,meta_user,meta_password,meta_driver,spark)
    return exist_schema_df


# loading the current schema details into the current_schema_df by using information_schema.columns
def fetch_current_schema_details_from_source(source,catalog_name,schema_name,tbl_name,user,password,driver,source_conn_string):
    if source == 'snowflake_uat':
      case = 'upper'
    else:
      case = 'lower'
    source_query = """SELECT distinct '{}:' || lower(table_catalog) || '.' || lower(table_schema) || '.' || lower(table_name) as "table_name",
                lower(column_name) AS "column_name", 
                lower(data_type) AS "data_type",
                lower(is_nullable) AS "is_nullable",
                (case when character_maximum_length  is null then 0 else character_maximum_length end) as "character_maximum_length",
                ordinal_position::integer AS "ordinal_position"
                FROM {}.information_schema.columns
                WHERE table_catalog = {}('{}')
                and table_schema ={}('{}')
                AND table_name = {}('{}') """
    current_schema = source_query.format(source,catalog_name,case,catalog_name,case,schema_name,case,tbl_name)
    current_schema_df = read_tbl_into_df(current_schema,catalog_name,source_conn_string,user,password,driver,spark)
    return current_schema_df
  
# Loading the anamoly data into the destination table 
def load_data_into_table(df,meta_conn_string):
  df_spark = spark.createDataFrame(df)
  insert_df_into_tbl(df_spark,'schema_anamoly',meta_conn_string)
      
def parDo_over_each_entry(row, credentials, meta_conn_string, meta_user, meta_password, meta_driver):
    config_id = row['config_id']

    # Step-3 : Fetch stored schema details from schema_configuration based on the table_name into a exist_schema_result_df
    log.info("Passing parameters to fetch_stored_schema_details_from_schema_configuration function for config_id = {config_id} !!!".format(config_id=config_id))
    exist_schema_result_df = fetch_stored_schema_details_from_schema_configuration(row['table_name'],meta_conn_string,meta_user,meta_password,meta_driver,spark) 

    # Step-4 : Fetch current schema details from source and store into a current_schema_result_df
    log.info("Passing parameters to fetch_current_schema_details_from_source function for config_id = {config_id} !!!".format(config_id=config_id))
    current_schema_result_df = fetch_current_schema_details_from_source(row['source'],row['catalog_name'], row['schema_name'], row['tbl_name'],credentials['db_'+row['source']]['username'],credentials['db_'+row['source']]['password'],credentials['db_'+row['source']]['driver'], credentials['db_'+row['source']]['conn_string'])

    # Step-5 : Compare that two df's and store that anamolies into a compare_schema_result_df
    log.info("Passing parameters to comparing_both_dataframes function for config_id = {config_id} !!!".format(config_id=config_id))
    compare_schema_result_df = comparing_both_dataframes(exist_schema_result_df,current_schema_result_df)
    if len(compare_schema_result_df)>0:
        compare_schema_result_df['config_id'] = config_id
        compare_schema_result_df['data_type'] = compare_schema_result_df['data_type'].astype(str)
        compare_schema_result_df['data_type_old'] = compare_schema_result_df['data_type'].astype(str)
        compare_schema_result_df['is_nullable'] = compare_schema_result_df['is_nullable'].astype(str)
        compare_schema_result_df['is_nullable_old'] = compare_schema_result_df['is_nullable'].astype(str)
        compare_schema_result_df.drop("table_name", axis=1, inplace=True)
        
    # Step-6 : Load the anamoly result into the schema_anamoly
        load_data_into_table(compare_schema_result_df,meta_conn_string)
    else:
      print('no anamoly data')
        

def schema_anamoly_detection(url,token,vault_meta_db):
  
    try:
    #   Step-1 : Fetching all the entries from master_configuration where check_type is schema
        master_config_query = '''SELECT table_name,config_id,
                                lower(split_part(table_name, ':', 1)) AS source,
                                lower(split_part(split_part(table_name, ':', 2), '.', 1)) AS catalog_name,
                                lower(split_part(split_part(table_name, ':', 2), '.', 2)) AS schema_name,
                                lower(split_part(split_part(table_name, ':', 2), '.', 3)) AS tbl_name
                                FROM 
                                    master_configuration 
                                WHERE 
                                    check_type = 'schema' '''

        master_configuration_result_df = read_tbl_into_df(master_config_query, 'meta_data',meta_conn_string,meta_user,meta_password,meta_driver,spark)
        master_configuration_result_df.apply(parDo_over_each_entry, axis=1, args=(credentials, meta_conn_string, meta_user, meta_password, meta_driver))
        log.info("Passing parameters to parDo_over_each_entry function for each entry in master_configuration")

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
  schema_anamoly_detection(url,token,vault_meta_db)
  write_logs(body=log_stringio.getvalue(), bucket=bucket_name, key=key)
