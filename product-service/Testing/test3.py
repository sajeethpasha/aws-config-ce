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
    # secret_name = "rdsgrafana_secrets"
    # region_name = "us-east-1"

    # Create a Boto3 session and client
    # session = boto3.session.Session()
    # client = session.client(
    #     service_name='secretsmanager',
    #     region_name=region_name
    # )

    # try:
    #     # Retrieve the secret value
    #     get_secret_value_response = client.get_secret_value(
    #         SecretId=secret_name
    #     )
    # except ClientError as e:
    #     raise e

    # Parse the secret string
    # secret = json.loads(get_secret_value_response['SecretString'])
    db_host = 'database-1.crwpj4y0tolc.ap-south-1.rds.amazonaws.com'
    db_port = '5432'
    db_user = 'postgres'
    db_password = 'Sajeeth123'
    db_name = 'postgres'

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    return conn    

 
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
    df.columns = [replace_special_chars_with_space(col) for col in df.columns ]
       
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

   
def replace_special_chars_with_space(text):
    chars_to_replace = "~!@#$%^&*()_+"
    for char in chars_to_replace:
        text = text.replace(char, " ").strip()
    return text   

def renameProductDf(df):
    df.rename(columns={'intelligent-trials-capabilities': 'capabilities',
                   'Intelligent Trials - Intelligent Trials Capabilities': 'Trials',
                   'Intelligent Trials - Admin': 'admin',
                   'Intelligent Trials Capabilities': 'TriCapabilities',
                   'Total costs': 'Totalcosts',
                   'Performance Analytics':'PerformanceAnalytics'},
          inplace=True, errors='raise')

def renameServiceDf(df):
    df.rename(columns={'Relational Database Service': 'capabilities',
                   'Elastic Load Balancing': 'loadbalancing',
                   'EC2-Instances': 'Ec2Instances',
                   'EC2-Other': 'Ec2Other',
                   'API Gateway': 'ApiGateWay',
                   'Total costs':'TotalCost',
                   'Elastic Container Service':'ElasticContainerService'},
          inplace=True, errors='raise')


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
    renameProductDf(merged_df)
    printD('coloumns names only Product',list(merged_df.columns))
    return merged_df


    

    
def process_services(services_latest_files):
    dfs = []
    for file in services_latest_files:
        df = read_file_to_dataframe(bucket_name, file)
        df=updteDf(df,'Service')
        file_name = file. split('/')[-1]
        source = file_name.split('_')[0]
        df['Source'] = [source] * len(df)
        
        dfs.append(df)
    merged_df = pd. concat(dfs, ignore_index = True) 
    merged_df.insert(0,'id', range(1, len(merged_df) + 1))
    merged_df. fillna(0.0, inplace = True)
    renameServiceDf(merged_df)
    printD('coloumns names only service',list(merged_df.columns))
    # renameServiceDf(merged_df)
    return merged_df

def create_table_with_schema(cursor):
    try:
        cursor.execute('''
 CREATE TABLE IF NOT EXISTS products (
    id INTEGER,
    Product TEXT,
    performsfapi double precision,
    capabilities double precision,
    intelligenttrialscapabilities double precision,
    intelligenttrialsprecompute double precision,
    intelligenttrialsapi double precision,
    edgesfui double precision,
    PerformanceAnalytics double precision,
    Trials double precision,
    admin double precision,
    TriCapabilities double precision,
    Totalcosts double precision,
    Source TEXT
);
        ''')
        cursor.execute('''
     
    CREATE TABLE IF NOT EXISTS services (
    id INTEGER,
    Service TEXT,
    ElastiCache double precision,
    Lambda double precision,
    capabilities double precision,
    loadbalancing double precision,
    Ec2Instances double precision,
    Ec2Other double precision,
    S3 double precision,
    SQS double precision,
    Inspector double precision,
    AppSync double precision,
    Shield double precision,
    ApiGateWay double precision,
    ElasticContainerService INTEGER,
    TotalCost double precision,
    Source TEXT
);

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
            # query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict_update}"
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            printD("query",query)
            cursor. execute(query, tuple(row))
            
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



def create_table_query(df, table_name):
    # Mapping from pandas dtype to SQL type
    dtype_mapping = {
        'int64': 'INTEGER',
        'float64': 'double precision',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'timedelta64[ns]': 'INTERVAL'
    }

    # Start the CREATE TABLE SQL statement
    create_table_sql = f"CREATE TABLE {table_name} (\n"
    
    # Loop over columns in the DataFrame to create column definitions
    for col in df.columns:
        col_name = col
        col_type = str(df[col].dtype)
        sql_type = dtype_mapping.get(col_type, 'TEXT')  # Default to TEXT if type is not found
        
        create_table_sql += f"    {col_name} {sql_type},\n"
    
    # Remove the last comma and add closing parenthesis
    create_table_sql = create_table_sql.rstrip(',\n') + "\n);"
    
    return create_table_sql


if __name__ == "__main__":
    try:
        # s3_client = boto3.client('s3')
        bucket_name = 'product-service-test'
        folder_prefix = 'product/'
        products_latest_files = get_latest_files(bucket_name, folder_prefix, 2)
        products_data = process_products(products_latest_files)
        printD('new Table',create_table_query(products_data,'products'))
        folder_prefix = 'services/'
        services_latest_files = get_latest_files(bucket_name, folder_prefix, 3)
        services_data = process_services(services_latest_files)
        printD('new Table',create_table_query(services_data,'services'))
        
        products_data.to_csv('productsfile.csv', header=False, index=False)
        services_data.to_csv('servicesfile.csv', header=False, index=False)
        print("product servci df ready")
        # conn = get_secret()
        print("Before cursor connection establish")
        # cursor = conn. cursor()
        print("Cursor connection established")
        # create_table_with_schema(cursor)
        # insert_data_into_postgresql(cursor, products_data, 'products')
        # insert_data_into_postgresql(cursor, services_data, 'services')
       
        
        # cursor. close()
        # conn.close
    except Exception as e:
        print(f"Error: {e}")