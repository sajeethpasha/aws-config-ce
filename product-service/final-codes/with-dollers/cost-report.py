import boto3 
import csv 
import psycopg2
import io
import pandas as pd 
from datetime import datetime
import json
from botocore.exceptions import ClientError

# def get_secret():
#     secret_name = "rdsgrafana_secret"
#     region_name = "us-east-1"
    
#     session = boto3.session.Session() 
#     client = session.client(
#         service_name='secretsmanager',
#         region_name=region_name
#     )
    
#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
#     except ClientError as e:
#         raise e

#     secret = json.loads(get_secret_value_response['SecretString'])
#     db_host = secret["host"]
#     db_port = secret["port"]
#     db_user = secret["username"]
#     db_password = secret["password"]
#     db_name = secret["dbname"]
#     conn = psycopg2.connect( 
#         host = db_host,
#         port = db_port,
#         dbname = db_name,
#         user = db_user,
#         password = db_password
#         )
        
#     return conn

def get_latest_files(bucket_name, folder_prefix, max_keys):
    response = s3_client.list_objects_v2(
        Bucket = bucket_name,
        Prefix = folder_prefix,
        MaxKeys = max_keys
        )
    files = [(obj['Key'], obj['LastModified']) for obj in response.get('Contents',[])]
    while 'NextContinuationToken' in response:
            response = s3_client.list_objects_v2(
                Bucket = bucket_name,
                Prefix = folder_prefix,
                MaxKeys = max_keys,
                ContinuationToken = response['NextContinuationToken']
                )
            files.extend([(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])])
    files = [file for file in files if file[0] != folder_prefix]
    sorted_files = sorted(files, key = lambda x:x[1], reverse = True)
    latest_files = [file[0] for file in sorted_files]
    print(latest_files)
    return latest_files

def read_file_to_dataframe(bucket_name, file_key):
    obj = s3_client.get_object(Bucket = bucket_name, Key = file_key)
    df = pd.read_csv(obj['Body'])
    df_modified = df[1:]
    return df_modified

def process_products(products_latest_files):
    dfs = []
    for file in products_latest_files:
        df = read_file_to_dataframe(bucket_name, file)
        file_name = file.split('/')[-1]
        source = file_name.split('_')[0]
        df['Source'] = [source] * len(df)
        dfs.append(df)
    merged_df = pd.concat(dfs, ignore_index = True)
    print("concat done")
    merged_df.insert(0, 'id', range(1, len(merged_df) + 1))
    merged_df.fillna(0.0, inplace=True)
    return merged_df

def fix_column_names(df):
    """
    This function replaces $, - and space with _.
    """
    print("before: ", df.columns)
    # new_cols = df.columns.str.replace(r'[($)]', '', regex=True)
    # new_cols = new_cols.str.replace(r'[\s-]', '_', regex=True)
    new_cols = df.columns.str.replace(r'[\s-]', '_', regex=True)
    new_cols = new_cols.str.replace(r'_+', '_', regex=True)
    new_cols = new_cols.str.lower()
    df.columns = new_cols
    print("after: ", df.columns)
    
    return df

def process_services(services_latest_files):
    dfs = []
    for file in services_latest_files:
        df = read_file_to_dataframe(bucket_name, file)
        file_name = file.split('/')[-1]
        source = file_name.split('_')[0]
        df['Source'] = [source] * len(df)
        dfs.append(df)
    merged_df = pd.concat(dfs, ignore_index = True) 
    merged_df.insert(0, 'id', range(1, len(merged_df) + 1))
    merged_df.fillna(0.0, inplace=True)
    return merged_df

def create_table_with_schema(cursor):
    try:
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS test_products (id SERIAL PRIMARY KEY,
                                                            Date date, 
                                                            capabilities double precision, 
                                                            icapabilities double precision, 
                                                            api double precision, 
                                                            ui double precision,
                                                            exapi double precision, 
                                                            performances double precision, 
                                                            capabilities double precision, 
                                                            cctv double precision, 
                                                            totalcosts double precision, 
                                                            producttrial double precision, 
                                                            productcapabilities double precision,
                                                            source text)
                    """)
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS test_services (id SERIAL PRIMARY KEY,
                                                Date date,
                                                capabilitiesservice double precision,
                                                icapabilities double precision,
                                                apiservice double precision,
                                                uiservice double precision,
                                                exapiservice double precision,
                                                performancesservice double precision,
                                                capabilitiesservice double precision,
                                                cctvservice double precision,
                                                totalcosts double precision,
                                                capabilitiesservice double precision,
                                                netservice double precision,
                                                appservice double precision,
                                                yieldservice double precision,
                                                gateservice double precision,
                                                services double precision,
                                                source text)
                    """)
        print("Tables are created successfully")
        cursor.connection.commit()
    except Exception as e:
        print("Error creating tables:", e)


def insert_data_into_postgresql(cursor, data, table_name):
    try:
        for index, row in data.iterrows():
            columns = ','.join(data.columns)
            placeholders = ','.join(['%s'] * len(row))
            conflict_columns = 'id'
            conflict_update = ','.join([f"{col} = excluded.{col}" for col in row.keys()])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict_update}"
            cursor.execute(query, tuple(row))
        cursor.connection.commit()
        print("Data inserted into Postgresql successfully")
    except Exception as e:
        print("Error inserting data in PostgreSQl:", e)


if __name__  == "__main__":
    try:
        s3_client = boto3.client('s3')
        bucket_name = 'report-poc-test'
        
        folder_prefix = 'raw/products/'
        products_latest_files = get_latest_files(bucket_name, folder_prefix, 2)
        products_data = process_products(products_latest_files)
        products_data = fix_column_names(products_data)
        
        print("write products dataframe to S3 location")
        products_data.to_csv("s3://report-poc-test/merged/merged.csv", header = True, index = False)
        
        folder_prefix = 'raw/services/'
        services_latest_files = get_latest_files(bucket_name, folder_prefix, 3)
        services_data = process_services(services_latest_files)
        services_data = fix_column_names(services_data)
        
        print("write services dataframe to S3 location")
        services_data.to_csv("s3://report-poc-test/merged/services.csv", header = True, index = False)
        
        # conn = get_secret()
        
        print("Before cursor connection establish")
        # cursor = conn.cursor()
        print("Cursor connection established")
        create_table_with_schema(cursor)
        insert_data_into_postgresql(cursor, products_data, 'test_products')
        insert_data_into_postgresql(cursor, services_data, 'test_services')
        cursor.close()
        conn.close
    except Exception as e:
        print(f"Error: {e}")