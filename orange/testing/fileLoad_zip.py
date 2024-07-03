import pandas as pd

# Path to the gzip-compressed CSV file
zip_file = r'D:\supports\akhil\orange_data\testing\Map-orange-cost-data.gz'

# Define the chunk size (number of rows per chunk)
chunk_size = 100000  # Adjust this based on your system's memory

# Specify the columns to load
columns_to_load = [
    'line_item_product_code', 
    'line_item_resource_id', 
    'line_item_unblended_cost', 
    'cost_category', 
    'line_item_usage_end_date', 
    'line_item_usage_start_date'
]

# Initialize an empty list to collect the processed chunks
chunks = []

try:
    # Read the CSV file in chunks
    for chunk in pd.read_csv(zip_file, compression='gzip', chunksize=chunk_size, usecols=columns_to_load):
        # Filter the chunk based on the condition in the 'cost_category' column
        filtered_chunk = chunk[chunk['cost_category'].str.contains('"product_tier_1":"Intelligent Trials"', na=False)]
       
        # Append the filtered chunk to the list
        chunks.append(filtered_chunk)

    # Concatenate all filtered chunks into a single DataFrame
    df = pd.concat(chunks, axis=0)

    # Now you can work with the entire DataFrame
    print("CSV file read and filtered successfully!")
    print(df.head())
    # Save the filtered DataFrame to a new CSV file
    df.to_csv(r'D:\supports\akhil\orange_data\testing\sample.csv', index=False)

except Exception as e:
    print(f"Error reading the CSV file: {e}")
