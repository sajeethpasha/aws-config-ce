import pandas as pd
import os

# Ensure the file path is correct
file_path = r'D:\supports\akhil\orange_data\testing\orange-data.csv'

# file_path = r'D:\supports\akhil\orange_data\testing\machine-2024.csv'

file_output =r'D:\supports\akhil\orange_data\out'


# Check if the file exists
if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
else:
    # Initialize an empty list to collect the filtered data
    filtered_data = []

    # Define the chunk size
    chunk_size = 100000  # Adjust the chunk size based on your system's memory

    try:
        # Read the CSV file in chunks
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            # Filter the chunk
            filtered_chunk = chunk[chunk['cost_category'].str.contains('"product_tier_1":"Intelligent Trials"', na=False)]
            # Append the filtered chunk to the list
            filtered_data.append(filtered_chunk[['line_item_product_code', 'line_item_resource_id', 'line_item_unblended_cost', 'cost_category','line_item_usage_end_date','line_item_usage_start_date']])
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")

    # Concatenate all the filtered chunks into a single DataFrame
    if filtered_data:
        result_df = pd.concat(filtered_data)
        print(result_df)
    else:
        print("No data matched the filter condition.")
    
    df = result_df.to_csv(r'file_output\data.csv', index=False)
        
     
        
