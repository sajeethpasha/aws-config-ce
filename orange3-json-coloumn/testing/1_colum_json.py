import pandas as pd
import json
from datetime import datetime
import os
import boto3
import io
from io import StringIO
import gzip

# Path to the gzip-compressed CSV file
zip_file = r'D:\supports\akhil\orange_data\testing\Map-orange-cost-data.gz'

# Define the chunk size (number of rows per chunk)
chunk_size = 100000  # Adjust this based on your system's memory

# Specify the columns to load
columns_to_load = [
    'cost_category',
    'resource_tags',
    'product',
    'identity_line_item_id',
    'identity_time_interval',
    'line_item_operation',
    'line_item_product_code'
]

# Function to check if the JSON contains a specific key
def contains_key(json_str, key):
    try:
        json_obj = json.loads(json_str)
        return key in json_obj
    except json.JSONDecodeError:
        return False

# Function to extract user_product from resource_tags
def extract_user_product(resource_tags):
    try:
        json_obj = json.loads(resource_tags)
        return json_obj.get('user_product', None)
    except json.JSONDecodeError:
        return None

# Initialize an empty list to collect the processed chunks
chunks = []

def get_cost():
    try:
        # Read the CSV file in chunks
        for chunk in pd.read_csv(zip_file, compression='gzip', chunksize=chunk_size, usecols=columns_to_load):
            chunks.append(chunk)

        df = pd.concat(chunks, axis=0)

        # Now you can work with the entire DataFrame
        print("CSV file read and filtered successfully!")
        print(df.head())
        
        # Generate the dynamic filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"col-json{timestamp}.csv"
        
        # Save the filtered DataFrame to a new CSV file with the dynamic filename
        df.to_csv(fr'D:\supports\akhil\orange_data\testing\out\col-json\{output_filename}', index=False)
        return df

    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None
    

def process_data():
    # Fetch data
    df = get_cost()
  
    

if __name__ == "__main__":
    try:
        print('Starting data processing...')
        process_data()
    except Exception as e:
        print(f"Error in processing data: {e}")
