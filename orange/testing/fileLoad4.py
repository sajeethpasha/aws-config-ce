import datetime
import pandas as pd
import os

file_path = r'D:\supports\akhil\orange_data\testing\data.csv'
output_dir = r'D:\supports\akhil\orange_data\out'

def get_file():
    # Ensure the file path is correct
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None
    else:
        return pd.read_csv(file_path)

def get_const(df):
    df['date'] = pd.to_datetime(df['line_item_usage_start_date']).dt.date
    df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    cost_data = df.to_dict(orient='records')
    return cost_data

# Usage
df = get_file()
if df is not None:
    cost_data = get_const(df)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Print the 'date' column for debugging
    print(df['date'])
    
        # Drop the unwanted columns
    df_dropped = df.drop(columns=['line_item_usage_end_date', 'line_item_usage_start_date'])

    # Reorder the columns
    df = df_dropped[['date', 'line_item_resource_id', 'line_item_product_code', 'line_item_unblended_cost']]
    
    # Group the DataFrame and sum the numeric columns
    df = df.groupby(['date', 'line_item_resource_id', 'line_item_product_code'], as_index=False).agg(
        {
            'line_item_unblended_cost': 'sum'
        }
    )



    # Convert the 'date' column to datetime format
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

    # Sort the data by the 'date' column in descending order
    df = df.sort_values(by='date', ascending=False)

    # Generate a dynamic filename based on the current date and time
    current_datetime = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    dynamic_filename = f'udate_date_{current_datetime}.csv'
    output_file = os.path.join(output_dir, dynamic_filename)

    # Save the sorted DataFrame to a CSV file
    df.to_csv(output_file, index=False)

    # Display the sorted DataFrame and the filename
    print(f"Sorted DataFrame saved to: {output_file}")
    print(df)
