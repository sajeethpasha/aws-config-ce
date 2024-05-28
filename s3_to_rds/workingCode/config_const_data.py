
import boto3
import gzip
import json
import io
import pandas as pd


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
                'tags': item['tags'],
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



def save_cost_to_csv_by_date(cost_data):
    cost_df = pd.DataFrame(cost_data)
    grouped = cost_df.groupby('line_item_usage_start_date_comparison')
    
    s3 = boto3.client('s3')
    bucket_name = 'mergedconfigandcostdata'
    
    for date, group in grouped:
        csv_buffer = io.StringIO()
        group.to_csv(csv_buffer, index=False)
        csv_key = f'cost_data/{date}.csv'
        try:
            s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
            print(f'Saved {csv_key} to S3')
        except Exception as e:
            print(f'Failed to save {csv_key} to S3: {e}')



def save_config_to_csv_by_date(config_data):
    config_df = pd.DataFrame(config_data)
    
    
    config_df_ok = config_df[config_df['configurationItemStatus'] == 'OK']
    config_df_ok = config_df_ok.sort_values(by=['configurationItemCaptureTime'], ascending=False)
    config_df_ok = config_df_ok.groupby(['configurationItemCaptureDate', 'resourceId']).head(1)
    
    config_df_not_ok = config_df[config_df['configurationItemStatus'] != 'OK']
    
    config_df_filtered = pd.concat([config_df_ok, config_df_not_ok])
    
    grouped = config_df_filtered.groupby('configurationItemCaptureDate')
    
    s3 = boto3.client('s3')
    bucket_name = 'mergedconfigandcostdata'
    
    for date, group in grouped:
        csv_buffer = io.StringIO()
        group.to_csv(csv_buffer, index=False)
        csv_key = f'config_data/{date}.csv'
        try:
            s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
            print(f'Saved {csv_key} to S3')
        except Exception as e:
            print(f'Failed to save {csv_key} to S3: {e}')


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



# def merge_data(config_data, cost_data):
#     config_df = pd.DataFrame(config_data)
#     cost_df = pd.DataFrame(cost_data)
    
#     merged_df = pd.merge(
#         config_df,
#         cost_df,
#         how='left',
#         left_on=['resourceId', 'configurationItemCaptureDate'],
#         right_on=['line_item_resource_id', 'line_item_usage_start_date_comparison']
#     )
    
#     merged_df.drop(columns=['line_item_usage_start_date_comparison'], inplace=True)
    
#     return merged_df

def save_merged_to_csv_by_date(merged_df):
    grouped = merged_df.groupby('configurationItemCaptureDate')
    
    s3 = boto3.client('s3')
    bucket_name = 'mergedconfigandcostdata'
    
    for date, group in grouped:
        group = group.drop(columns=['configurationItemCaptureDate'])
        csv_buffer = io.StringIO()
        group.to_csv(csv_buffer, index=False)
        csv_key = f'merged_results/{date}.csv'
        try:
            s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
            print(f'Saved {csv_key} to S3')
        except Exception as e:
            print(f'Failed to save {csv_key} to S3: {e}')
            
            

def lambda_handler(event, context):
    try:
        config_data = get_config(event, context)
        cost_data = get_cost(event, context)
        # printD('config_data', config_data)
        # printD('cost_data', cost_data)
        
        # Merge the data and save the merged data to CSV files
        merged_df = merge_data(config_data, cost_data)
        printD('merged_data', merged_df)
        # merged_df.to_csv('sajeeth.csv', sep=',', index = False, encoding='utf-8')
        print(merged_df)
        # save_merged_to_csv_by_date(merged_df)
        
        print("Config and Cost Data saved to separate CSV files, and merged data saved to CSV files by date.")
        return {"status": "success"}
    except Exception as e:
        print(f'Error: {e}')
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    event = {}
    context = {}
    lambda_handler(event, context)

