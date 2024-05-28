import boto3 
import csv 
import psycopg2
import io
import pandas as pd 
from datetime import datetime import json
from botocore. exceptions import clientError

def get_secret:
    secret_name = "rdsgrafana_ secrets"
    region_name = "us-east-1"
    session = boto3. session.Session client = session. client (
    service_name = 'secretsmanager", region_name = region_name )
    try:

    get_secret_value_response = client.get_secret_value
    (
    SecretId = secret_name)

    except ClientError as e:
    raise e

    secret = json. Loads(get_secret_value_response['SecretString'])
    db_host = secret["host"]
    db_port = secret["port"]
    db_user = secret["username"]
    db_password = secret["password"]
    db_name = secret["dbname"]
    conn = psycopg2. connect ( 
    host = db_ host,
    port = db_port,
    dbname = db_name,
    user = db_user,
    password = db_password
    )
return conn

def get_latest_files(bucket_name, folder_prefix, max_keys) :

    response = S3_client.list_objects_v2(
    Bucket = bucket_name,
    Prefix = folder_prefix,
    MaKeys = max_keys
    )
    files = [(obj['Key'], obj ['LastModified']) for obj in response.get('Contents', [])
    while
    'NextContinuationToken' in response:

    response = s3_client.list_objects_v2(
    Bucket = bucket_name,
    Prefix - folder_prefix,
    MaKeys = max_keys,
    ContinuationToken = response['NextContinuationToken’]
    )
    files. extend([(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])])
    files = [file for file in files if file[0] != folder_prefix]
    sorted_files = sorted(files, key = lambda x:x[1], reverse = True)
    latest_files = [file[0] for file in sorted_files]
    return latest_files

    def read_file_to_dataframe(bucket_name, file_key):
    obj = s3_client.get_object(Bucket = bucket_name, Key = file_key)
    df = pd.read_csv(obj['Body'])
    return df

def process products(products_latest_files):
    dfs = []
    for file in products_ latest files:
    df = read_file_to_ dataframe(bucket name, file)
    file_name = file. split('/‘)[-1]
    source = file_name.split(‘_’)[0]
    df['Source] = [source] * len(df)
    dfs.append(df)
    merged df = pd. concat(dfs, ignore index = True) 
    merged df.insert(0,’id', range(1, len(merged_df) + 1)
    merged_df. fillna(0.0, inplace = True)
return merged_df

def process_services(services_latest_files):
    dfs = []
    for file in services_ latest files:
    df = read_file_to_ dataframe(bucket name, file)
    file_name = file. split('/‘)[-1]
    source = file_name.split(‘_’)[0]
    df['Source] = [source] * len(df)
    dfs.append(df)
    merged df = pd. concat(dfs, ignore index = True) 
    merged df.insert(0,’id', range(1, len(merged_df) + 1)
    merged_df. fillna(0.0, inplace = True)
    return merged_df

def create_table_with_schema(cursor):
    try:
    cursor.execute(“””
    CREATE TABLE IF NOT EXISTS test_ products (id SERIAL PRIMARY KEY,
    Date date, capabilities double precision, icapabilities double precision, api double precision, ui double precision,
    exapi double precision, performances double precision, capabilities double precision, cctv double precision, totalcosts double precision, producttrial double precision, productcapabilities double precision, source text)
    “””)
    Cursor.execute(“””
    CREATE TABLE IF NOT EXISTS test_ services (id SERIAL PRIMARY KEY,
    Date date, capabilitiesservice double precision, icapabilities double precision, apiservice double precision, uiservice double precision, exapiservice double precision, performancesservice double precision, capabilitiesservice double precision, cctvservice double precision, totalcosts double precision, capabilitiesservice double precision, netservice double precision, appservice double precision,
    yieldservice double precision,
    gateservice double precision,
    services double precision, source text)
    “””)
    print("Tables are created successfully")
    cursor.connection.commit()
    except Exception as e:
    print("Error creating tables:", e)


def insert_data_into_postgresql(cursor, data, table_name) :
    try:
    for index, row in data. iterrows):
    columns = ‘,’.join(data.columns)
    placeholders = ‘,’.join(['%s'] * len(row))
    Conflict_columns = ‘id’
    conflict_update = 
    join([f"{col) =
    excluded. (col)" for col in row. keys()])
    query = f "INSERT INTO (table_name) ({columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict _update}"
    cursor. execute(query, tuple(row))
    cursor.connection.commit()
    print("Data inserted into Postgresql successfully")
    except Exception as e:
    print("Error inserting data in PostgreSQl:", e)