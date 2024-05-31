import boto3 
import csv 
import psycopg2
import io
import pandas as pd 
from datetime import datetime

import json
from botocore. exceptions import ClientError


s3_client = boto3.client('s3')

# def get_secret:
#     secret_name = "rdsgrafana_ secrets"
#     region_name = "us-east-1"
#     session = boto3. session.Session client = session. client (
#     service_name = 'secretsmanager", region_name = region_name
#     )
#     try:

#         get_secret_value_response = client.get_secret_value
#         (
#         SecretId = secret_name)

#     except ClientError as e:
#         raise e

#     secret = json. Loads(get_secret_value_response['SecretString'])
#     db_host = secret["host"]
#     db_port = secret["port"]
#     db_user = secret["username"]
#     db_password = secret["password"]
#     db_name = secret["dbname"]
#     conn = psycopg2. connect ( 
#         host = db_ host,
#         port = db_port,
#         dbname = db_name,
#         user = db_user,
#         password = db_password
#         )
#     return conn

def get_secret():
    secret_name = "rdsgrafana_secrets"
    region_name = "us-east-1"

    # Create a Boto3 session and client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        # Retrieve the secret value
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Parse the secret string
    secret = json.loads(get_secret_value_response['SecretString'])
    db_host = secret["host"]
    db_port = secret["port"]
    db_user = secret["username"]
    db_password = secret["password"]
    db_name = secret["dbname"]

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )    

 
def printD(name,object):
    print('-------------------'+name+'---------------------------------')
    print(object)
    print('-----------------------------------------------------')
    
def get_latest_files(bucket_name, folder_prefix, max_keys):
    print(bucket_name)
    # s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(
    Bucket = bucket_name,
    Prefix = folder_prefix,
    MaxKeys = max_keys
    )
    files = [(obj['Key'], obj ['LastModified']) for obj in response.get('Contents', []) ]
    while 'NextContinuationToken' in response:
            response = s3_client.list_objects_v2(
                Bucket = bucket_name,
                Prefix = folder_prefix,
                MaxKeys = max_keys,
                ContinuationToken = response['NextContinuationToken']
            )
            files. extend([(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])])
   
    # printD('files',files)
    files = [file for file in files if file[0] != folder_prefix]
    sorted_files = sorted(files, key = lambda x:x[1], reverse = True)
    latest_files = [file[0] for file in sorted_files]
    print(latest_files)
    return latest_files

def read_file_to_dataframe(bucket_name, file_key):
    obj = s3_client.get_object(Bucket = bucket_name, Key = file_key)
    df = pd.read_csv(obj['Body'])
    return df


def validate(date_text):
    try:
        datetime.strptime(str(date_text), '%Y-%m-%d')
        return True
    except ValueError:
        return False
    
def updteDf(df,clm):
    condition = df[clm].apply(validate)
    return df[condition]

   

def process_products(products_latest_files):
    dfs = []
    printD('products_latest_files',products_latest_files)
    for file in products_latest_files:
        df = read_file_to_dataframe(bucket_name, file)
        df=updteDf(df,'Product')
        file_name = file.split('/')[-1]
        source = file_name.split('_')[0]
        df['Source'] = [source] * len(df)
        dfs.append(df)
    merged_df = pd. concat(dfs, ignore_index = True) 
    merged_df.insert(0,'id', range(1, len(merged_df) + 1))
    merged_df.fillna(0.0, inplace = True)
    printD('merged_df',merged_df)
    return merged_df


    

    
def process_services(services_latest_files):
    dfs = []
    for file in services_latest_files:
        df = read_file_to_dataframe(bucket_name, file)
        df=updteDf(df,'Service')
        file_name = file. split('/')[-1]
        source = file_name.split('_')[0]
        df['Source'] = [source] * len(df)
        updteDf(df)
        dfs.append(df)
    merged_df = pd. concat(dfs, ignore_index = True) 
    merged_df.insert(0,'id', range(1, len(merged_df) + 1))
    merged_df. fillna(0.0, inplace = True)
    return merged_df

def create_table_with_schema(cursor):
    try:
        cursor.execute('''
                        CREATE TABLE IF NOT EXISTS test_ products (id SERIAL PRIMARY KEY,
        Date date, capabilities double precision, icapabilities double precision, api double precision, ui double precision,
        exapi double precision, performances double precision, capabilities double precision, cctv double precision, totalcosts double precision, producttrial double precision, productcapabilities double precision, source text)
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS test_ services (id SERIAL PRIMARY KEY,
        Date date, capabilitiesservice double precision, icapabilities double precision, apiservice double precision, uiservice double precision, exapiservice double precision, performancesservice double precision, capabilitiesservice double precision, cctvservice double precision, totalcosts double precision, capabilitiesservice double precision, netservice double precision, appservice double precision,
        yieldservice double precision,
        gateservice double precision,
        services double precision, source text)
        ''')
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
            conflict_update = ','.join([f"{col} = excluded.(col)" for col in row. keys()])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict_update}"
            # cursor. execute(query, tuple(row))
            printD("query",query)
        cursor.connection.commit()
        print("Data inserted into Postgresql successfully")
    except Exception as e:
        print("Error inserting data in PostgreSQl:", e)


def insert_data_into_postgresql2( data, table_name):
    print("Inserting data into Post")
    try:
        for index, row in data.iterrows():
            columns = ','.join(data.columns)
            placeholders = ','.join(['%s'] * len(row))
            conflict_columns = 'id'
            conflict_update = ','.join([f"{col} = excluded.(col)" for col in row. keys()])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict_update}"
            # cursor. execute(query, tuple(row))
            printD("query",query)
        
        print("Data inserted into Postgresql successfully")
    except Exception as e:
        print("Error inserting data in PostgreSQl:", e)

if __name__ == "__main__":
    try:
        # s3_client = boto3.client('s3')
        bucket_name = 'product-service'
        folder_prefix = 'product/'
        products_latest_files = get_latest_files(bucket_name, folder_prefix, 2)
        products_data = process_products(products_latest_files)
        # folder_prefix = 'services/'
        # services_latest_files = get_latest_files(bucket_name, folder_prefix, 3)
        # services_data = process_services(services_latest_files)
        # print("product servci df ready")
        # conn = get_secret()
        print("Before cursor connection establish")
        # cursor = conn. cursor()
        print("Cursor connection established")
        # create_table_with_schema(cursor)
        # insert_data_into_postgresql(cursor, products_data, 'test_products')
        # insert_data_into_postgresql(cursor, services_data, 'test_services')
        # insert_data_into_postgresql2(products_data, 'test_services')
        
        # cursor. close()
        # conn.close
    except Exception as e:
        print(f"Error: {e}")