import datetime
import pandas as pd
import os
import boto3
import io
import psycopg2


bucket = 'orange-123'
output_dir = r'out-files/'

# Initialize boto3 client for S3
s3 = boto3.client('s3')

def get_secret():
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

def create_table_with_schema(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monlty_report (
                id SERIAL PRIMARY KEY,
                product TEXT,
                account TEXT,
                account_id TEXT,
                service TEXT,
                cost double precision,
                total_product_cost double precision,
                date DATE,
                CONSTRAINT unique_record UNIQUE (product, account, account_id, service, date)
            );
        """)
        print("Table 'monlty_report' created successfully")
        cursor.connection.commit()
    except Exception as e:
        print("Error creating table:", e)

def insert_data_into_postgresql(cursor, data, table_name):
    try:
        for index, row in data.iterrows():
            columns = ','.join(data.columns)
            placeholders = ','.join(['%s'] * len(row))
            conflict_columns = ['product', 'account', 'account_id', 'service', 'date']  # Assuming these columns define uniqueness
            conflict_update = ','.join([f"{col} = excluded.{col}" for col in data.columns])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {conflict_update}"
            cursor.execute(query, tuple(row))
        cursor.connection.commit()
        print("Data inserted into PostgreSQL successfully")
    except Exception as e:
        print("Error inserting data into PostgreSQL:", e)
        
        


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