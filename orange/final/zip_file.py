import pandas as pd
import os
import boto3
import io
from io import StringIO
import gzip

# S3 bucket details
bucket = 'orange-123'
output_dir = r'out-files/'

# Initialize boto3 client for S3
s3 = boto3.client('s3')

def get_cost():
    key = 'input-files/orange_data_zip_file.csv.gz'
    response = s3.get_object(Bucket=bucket, Key=key)
    gzipped_content = response['Body'].read()
    
    with gzip.GzipFile(fileobj=io.BytesIO(gzipped_content)) as gz_file:
        # Define chunk size
        chunk_size = 100000  # Adjust this based on your system's memory
        chunks = []
        
        for chunk in pd.read_csv(gz_file, chunksize=chunk_size):
            # Append the chunk to the list of chunks
            chunks.append(chunk)
        
        # Concatenate all chunks into a single DataFrame
        df = pd.concat(chunks, axis=0)
    
    print("Data extracted and concatenated...")
    
    # Add a date column
    df['date'] = df['line_item_usage_start_date'].apply(lambda x: x[:10])
    print("Date column added...")

    return df

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

def process_data():
    # Fetch data
    df = get_cost()
    save_to_s3(df, 'zip_out_file_test.csv')

if __name__ == "__main__":
    try:
        print('Starting data processing...')
        process_data()
    except Exception as e:
        print(f"Error in processing data: {e}")
