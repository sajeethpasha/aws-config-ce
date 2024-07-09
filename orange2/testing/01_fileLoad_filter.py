import pandas as pd
import json
import pandas as pd
import json
from datetime import datetime

# Path to the gzip-compressed CSV file
zip_file = r'D:\supports\akhil\orange_data\testing\Map-orange-cost-data.gz'

# Define the chunk size (number of rows per chunk)
chunk_size = 100000  # Adjust this based on your system's memory

# Specify the columns to load
columns_to_load = [
    'cost_category',
    'resource_tags',
    'line_item_usage_start_date', 
    'line_item_resource_id', 
    'line_item_product_code', 
    'line_item_unblended_cost',
    'resource_tags'
]

# Function to check if the JSON contains a specific key
def contains_key(json_str, key):
    try:
        json_obj = json.loads(json_str)
        return key in json_obj
    except json.JSONDecodeError:
        return False

# Initialize an empty list to collect the processed chunks
chunks = []

try:
    # Read the CSV file in chunks
    for chunk in pd.read_csv(zip_file, compression='gzip', chunksize=chunk_size, usecols=columns_to_load):
        # Filter the chunk based on the condition in the 'cost_category' column
        filtered_chunk = chunk[chunk['cost_category'].str.contains('"product_tier_1":"Intelligent Trials"', na=False)]
        
        # Further filter the chunk based on 'resource_tags' containing the key 'user_product'
        filtered_chunk = filtered_chunk[filtered_chunk['resource_tags'].apply(lambda x: contains_key(x, "user_product"))]
        
        # Append the filtered chunk to the list
        chunks.append(filtered_chunk)

    # Concatenate all filtered chunks into a single DataFrame
    df = pd.concat(chunks, axis=0)

    # Add a 'date' column
    df['date'] = df['line_item_usage_start_date'].apply(lambda x: x[:10])
    
    # Now you can work with the entire DataFrame
    print("CSV file read and filtered successfully!")
    print(df.head())
    
    
     # Generate the dynamic filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"filter_sample2_{timestamp}.csv"
    
    # Save the filtered DataFrame to a new CSV file with the dynamic filename
    df.to_csv(fr'D:\supports\akhil\orange_data\testing\out\{output_filename}', index=False)


except Exception as e:
    print(f"Error reading the CSV file: {e}")
