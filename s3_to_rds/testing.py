
import boto3
import gzip
import json
import io
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from io import StringIO



def printD(name,object):
    print('-------------------'+name+'---------------------------------')
    print(object)
    print('-----------------------------------------------------')

def list_s3_objects(bucket_name, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    object_keys = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.json.gz'):
                    object_keys.append(obj['Key'])
    return object_keys

def read_gzipped_json_from_s3(bucket_name, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    gzipped_content = response['Body'].read()
    
    with gzip.GzipFile(fileobj=io.BytesIO(gzipped_content)) as gz_file:
        json_content = gz_file.read()
        decoded_content = json_content.decode('utf-8')
        json_data = json.loads(decoded_content)
    
    return json_data

def get_config(event, context):
    bucket_name = 'config-bucket-016266444216'
    prefix = 'AWSLogs/016266444216/Config/ap-south-1/2024/5/'
    
    object_keys = list_s3_objects(bucket_name, prefix)
    config_data = []

    for key in object_keys:
        print(f'Reading {key}')
        json_data = read_gzipped_json_from_s3(bucket_name, key)
        
        for item in json_data['configurationItems']:
            config_entry = {
                # 'tags': item['tags'],
                'configurationItemVersion': item['configurationItemVersion'],
                'configurationItemCaptureTime': item['configurationItemCaptureTime'],
                'configurationItemCaptureDate': item['configurationItemCaptureTime'][:10],  
                'configurationStateId': item['configurationStateId'],
                'awsAccountId': item['awsAccountId'],
                'configurationItemStatus': item['configurationItemStatus'],
                'resourceType': item['resourceType'],
                'resourceId': item['resourceId'],
                'awsRegion': item['awsRegion']
            }
            config_data.append(config_entry)
    
    return config_data


def get_cost(event, context):
    s3 = boto3.client('s3')
    bucket = 'akhil-s3-bucket-760'
    key = 'cost//sample-data-export/data/BILLING_PERIOD=2024-05/sample-data-export-00001.csv.gz'
    response = s3.get_object(Bucket=bucket, Key=key)
    gzipped_content = response['Body'].read()
    
    with gzip.GzipFile(fileobj=io.BytesIO(gzipped_content)) as gz_file:
        csv_content = gz_file.read()
        df = pd.read_csv(io.BytesIO(csv_content))
    
    columns_to_keep = [
        'identity_time_interval',
        'bill_billing_period_start_date',
        'bill_billing_period_end_date',
        'line_item_usage_start_date',
        'line_item_usage_end_date',
        'line_item_product_code',
        'line_item_operation',
        'line_item_resource_id',
        'line_item_unblended_rate',
        'line_item_unblended_cost',
        'line_item_blended_rate',
        'line_item_blended_cost',
        'line_item_usage_amount'
    ]
    df = df[columns_to_keep]
    
    df['line_item_usage_start_date_comparison'] = df['line_item_usage_start_date'].apply(lambda x: x[:10])
    
    cost_data = df.to_dict(orient='records')    
    return cost_data


def merge_data(config_data, cost_data):
    config_df = pd.DataFrame(config_data)
    cost_df = pd.DataFrame(cost_data)
    
    
    config_df_ok = config_df[config_df['configurationItemStatus'] == 'OK']
    config_df_ok = config_df_ok.sort_values(by=['configurationItemCaptureTime'], ascending=False)
    config_df_ok = config_df_ok.groupby(['configurationItemCaptureDate', 'resourceId']).head(1)
    
    config_df_not_ok = config_df[config_df['configurationItemStatus'] != 'OK']
    
    config_df_filtered = pd.concat([config_df_ok, config_df_not_ok])
    
    merged_df = pd.merge(
        config_df_filtered,
        cost_df,
        how='left',
        left_on=['resourceId', 'configurationItemCaptureDate'],
        right_on=['line_item_resource_id', 'line_item_usage_start_date_comparison']
    )
    
    merged_df.drop(columns=['line_item_usage_start_date_comparison'], inplace=True)
    
    return merged_df



# ***************DB Configuration and insert records****************
def insert_db_records(df):
    db_host = "database-1.crwpj4y0tolc.ap-south-1.rds.amazonaws.com"
    db_name = "postgres"
    db_user = "postgres"
    db_password = "Sajeeth123"
    db_port = "5432"

    # Create a connection to the PostgreSQL database
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}')
    df.to_sql('confcost', con=engine, if_exists='append', index=False)
    print("DataFrame uploaded successfully.")

# ************************************************************************************************   

# def table_creatin():
    
#     conn = psycopg2.connect(
#         dbname=dbname,
#         user=username,
#         password=password,
#         host=endpoint,
#         port=5432
#     )


#     cur = conn.cursor()
#     cur.execute("""
#             id SERIAL PRIMARY KEY
#             create table query
#             CREATE TABLE "confcost" (
#             "tags" text,
#             "configurationItemVersion" double precision,
#             "configurationItemCaptureTime" text,
#             "configurationItemCaptureDate" text,
#             "configurationStateId" bigint,
#             "awsAccountId" bigint,
#             "configurationItemStatus" text,
#             "resourceType" text,
#             "resourceId" text,
#             "awsRegion" text,
#             "identity_time_interval" text NULL,
#             "bill_billing_period_start_date" text NULL,
#             "bill_billing_period_end_date" text NULL,
#             "line_item_usage_start_date" text NULL,
#             "line_item_usage_end_date" text NULL,
#             "line_item_product_code" text NULL,
#             "line_item_operation" text NULL,
#             "line_item_resource_id" text NULL,
#             "line_item_unblended_rate" text NULL,
#             "line_item_unblended_cost" text NULL,
#             "line_item_blended_rate" text NULL,
#             "line_item_blended_cost" text NULL,
#             "line_item_usage_amount" text NULL
#             );
#     """)
#     conn.commit()

#     # Close the cursor and connection
#     cur.close()
#     conn.close()

# ************************************************************************************************   
 
def psychopg_insert(df):
    db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Sajeeth123',
    'host': 'database-1.crwpj4y0tolc.ap-south-1.rds.amazonaws.com',
    'port': '5432'
     }
        
    # Establish a connection to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()


    # Insert DataFrame data into PostgreSQL
    for i, row in df.iterrows():
        
        printD('row',row['resourceType'])
        insert_query = """
        INSERT INTO public.confcost (resourceType,configurationItemVersion,awsAccountId)
        VALUES (%s,%s,%s)
        """
        conv = lambda i : i or ''
        cursor.execute(insert_query, [
                            conv(row['resourceType']),
                            conv(row['configurationItemVersion']),
                            conv(row['awsAccountId'])
                 ] )

    # # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
 
def testing():
    db_params = {
    'dbname': 'confcost',
    'user': 'postgres',
    'password': 'Sajeeth123',
    'host': 'database-1.crwpj4y0tolc.ap-south-1.rds.amazonaws.com',
    'port': '5432'
     }
    
    conn = psycopg2.connect(host='database-1.crwpj4y0tolc.ap-south-1.rds.amazonaws.com',
                        port= '5432',
                        user= 'postgres',
                        password= 'Sajeeth123',
                        database= 'postgres') 
        
    # Establish a connection to the database
    # conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO public.confcost (resourceType) VALUES(%s)", ('5'))
    conn.commit() 


        
    # # printD('row',row['resourceType'])
    # insert_query = """
    # INSERT INTO public.confcost (resourceType)
    # VALUES (%s)
    # """
    # cursor.execute(insert_query, ['5'])
    # conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
     

# def insert_data_into_postgresql(cursor, data, table_name) :
#     try:
#         for index, row in data. iterrows):
#             columns = ‘,’.join(data.columns)
#             placeholders = ‘,’.join(['%s'] * len(row))
#             Conflict_columns = ‘id’
#             conflict_update = 
#             join([f"{col) =
#             excluded. (col)" for col in row. keys()])
#             query = f "INSERT INTO (table_name) ({columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict _update}"
#             cursor. execute(query, tuple(row))
#             cursor.connection.commit()
#             print("Data inserted into Postgresql successfully")
#     except Exception as e:
#         print("Error inserting data in PostgreSQl:", e)        

def lambda_handler(event, context):
    try:
        # table_creatin()
        config_data = get_config(event, context)
        cost_data = get_cost(event, context)
        # printD('config_data', config_data)
        # printD('cost_data', cost_data)
        
        # Merge the data and save the merged data to CSV files
        merged_df = merge_data(config_data, cost_data)
        # insert_db_records(merged_df)
        psychopg_insert(merged_df)
        printD('merged_data', merged_df)
        
        
        print("Config and Cost Data saved to separate CSV files, and merged data saved to CSV files by date.")
        return {"status": "success"}
    except Exception as e:
        print(f'Error: {e}')
        return {"status": "error", "message": str(e)}
    
    
    
    

if __name__ == "__main__":
    event = {}
    context = {}
    lambda_handler(event, context)
    # testing()

