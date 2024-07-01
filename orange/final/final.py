import datetime
import pandas as pd
import os
import boto3
import io
from io import StringIO
import gzip

file_path = r'input-files/orange_data.csv'
output_dir = r'out-files/'
bucket = 'orange-123'

# Initialize boto3 client for S3
s3 = boto3.client('s3')


def get_cost(event, context):
    s3 = boto3.client('s3')
    bucket = 'akhil-s3-bucket-760'
    key = 'cost//sample-data-export/data/BILLING_PERIOD=2024-05/sample-data-export-00001.csv.gz'
    response = s3.get_object(Bucket=bucket, Key=key)
    gzipped_content = response['Body'].read()
    
    with gzip.GzipFile(fileobj=io.BytesIO(gzipped_content)) as gz_file:
        csv_content = gz_file.read()
        df = pd.read_csv(io.BytesIO(csv_content))
    
    df = df[df['cost_category'].str.contains('"product_tier_1":"Intelligent Trials"', na=False)]
    columns_to_keep = [
        'line_item_product_code', 'line_item_resource_id', 'line_item_unblended_cost', 'cost_category', 'line_item_usage_end_date', 'line_item_usage_start_date'
    ]
    df = df[columns_to_keep]
    
    df['date'] = df['line_item_usage_start_date'].apply(lambda x: x[:10])
    
    cost_data = df.to_dict(orient='records')    
    return cost_data


# def get_file():
#     # Ensure the file path is correct
#     # Check if the file exists
#     if not os.path.exists(file_path):
#         print(f"File not found: {file_path}")
#         return None
#     else:
#         return pd.read_csv(file_path)

# def get_file_s3():
#     try:
#         response = s3.get_object(Bucket=bucket, Key=file_path)
#         csv_content = response['Body'].read()
#         csv_file = io.BytesIO(csv_content)
#         return pd.read_csv(csv_file)
#     except Exception as e:
#         print(f"Error reading file from S3: {e}")
#         return None

def add_date(df):
    df['date'] = pd.to_datetime(df['line_item_usage_start_date']).dt.date
    df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

def save_to_s3(df, filename):
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()
    filepath = os.path.join(output_dir, filename)

    try:
        s3.put_object(Bucket=bucket, Key=filepath, Body=csv_content)
        print(f'Saved {filename} to S3')
    except Exception as e:
        print(f'Failed to save {filename} to S3: {e}')

def proccess_data():
    # Fetch data
    df = get_cost()
    if df is None:
        print("no data available")
        return
    
    # Add date column
    add_date(df)
    
    # Apply transformations
    df = df.drop(columns=['line_item_usage_end_date', 'line_item_usage_start_date'])
    df = df[['date', 'line_item_resource_id', 'line_item_product_code', 'line_item_unblended_cost']]
    df = df.groupby(['date', 'line_item_resource_id', 'line_item_product_code'], as_index=False).agg(
        {
            'line_item_unblended_cost': 'sum'
        }
    )
    df['product_cost'] = df.groupby(['date', 'line_item_product_code'])['line_item_unblended_cost'].transform('sum')
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df = df.sort_values(by='date', ascending=False)
    
    # Save to CSV in S3
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    current_datetime = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    dynamic_filename = f'update_date_{current_datetime}.csv'
    save_to_s3(df, dynamic_filename)
    
    print(f"Sorted DataFrame saved to S3: {dynamic_filename}")
    print(df)

if __name__ == "__main__":
    try:
        print('Starting data processing...')
        proccess_data()
    except Exception as e:
        print(f"Error in processing data: {e}")
