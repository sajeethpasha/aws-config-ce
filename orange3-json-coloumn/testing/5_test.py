import pandas as pd
import json
from datetime import datetime
import os

# Define the path to the gzip-compressed CSV file
zip_file = r'D:\supports\akhil\orange_data\testing\Map-orange-cost-data.gz'

# Define the chunk size (number of rows per chunk)
chunk_size = 100000  # Adjust this based on your system's memory

# Specify the columns to load
columns_to_load = [
    'cost_category',
    'resource_tags',
    'product' 
]

# Function to normalize JSON data and add prefixes to keys
def normalize_json_data(row):
    cost_category = json.loads(row['cost_category']) if pd.notna(row['cost_category']) else {}
    product = json.loads(row['product']) if pd.notna(row['product']) else {}
    resource_tags = json.loads(row['resource_tags']) if pd.notna(row['resource_tags']) else {}
    
    combined = {
        **{f'cc_{k}': v for k, v in cost_category.items()},
        **{f'product_{k}': v for k, v in product.items()},
        **{f'rt_{k}': v for k, v in resource_tags.items()},
        'cost_category': json.dumps(cost_category),
        'product': json.dumps(product),
        'resource_tags': json.dumps(resource_tags)
    }
    
    return combined

# Initialize an empty list to collect the processed chunks
chunks = []

def get_cost():
    try:
        # Read the CSV file in chunks
        for chunk in pd.read_csv(zip_file, compression='gzip', chunksize=chunk_size, usecols=columns_to_load):
            # Normalize and prefix JSON data
            normalized_chunk = chunk.apply(lambda row: normalize_json_data(row), axis=1)
            normalized_chunk_df = pd.json_normalize(normalized_chunk)
            chunks.append(normalized_chunk_df)

        # Concatenate all chunks into a single DataFrame
        df = pd.concat(chunks, axis=0)

        # Generate the dynamic filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"col-json-{timestamp}.csv"

        # Save the filtered DataFrame to a new CSV file with the dynamic filename
        output_path = fr'D:\supports\akhil\orange_data\testing\out\col-json\{output_filename}'
        df.to_csv(output_path, index=False)

        print("CSV file read, processed, and saved successfully!")
        return df

    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None

def process_data():
    # Fetch data
    df = get_cost()
    if df is not None:
        print(df.head())

if __name__ == "__main__":
    try:
        print('Starting data processing...')
        process_data()
    except Exception as e:
        print(f"Error in processing data: {e}")
