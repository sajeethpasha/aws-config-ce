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
    # df['date'] = df['line_item_usage_start_date'].apply(lambda x: x[:10])
    # df['date'] = pd.to_datetime(df['line_item_usage_start_date']).dt.strftime('%Y-%m-%d')
    df['date'] = pd.to_datetime(df['line_item_usage_start_date']).dt.date
    df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

    cost_data = df.to_dict(orient='records')
    return cost_data

# Usage
df = get_file()
if df is not None:
    cost_data = get_const(df)
    # print(cost_data)
    # Define the output directory and ensure it exists
    # output_dir = r'D:\supports\akhil\orange_data\out'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # Save the result DataFrame to a CSV file in the output directory
    # output_file = os.path.join(output_dir, 'new_data.csv')
    # df.to_csv(output_file, index=False)
    
    # line_item_usage_start_date_comparison
    df_grouped = df.groupby(['date', 'line_item_resource_id','line_item_product_code'], as_index=False).sum()
    # Drop the unwanted columns
    df_dropped = df.drop(columns=['line_item_usage_end_date', 'line_item_usage_start_date'])
    
    # Reorder the columns
    df = df_dropped[['date', 'line_item_resource_id', 'line_item_product_code','line_item_unblended_cost']]
    # df=df_reordered

    # print( df['cost_category'].unique())
    
    # Convert the 'date' column to datetime format if it's not already
    # df.loc[:, 'date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

    # Sort the DataFrame by the 'date' column
    # df_sorted  = df.sort_values(by='date')
    
    # Convert the 'date' column to datetime
    df[:,'date'] = pd.to_datetime(df['date'], dayfirst=True)

    # Sort the data by the 'date' column in descending order
    df = df.sort_values(by='date', ascending=False)

    # Display the sorted DataFrame
    # print(df_sorted)

    # print(df.columns)
    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    dynamic_filename = f'udate_date_{current_datetime}.csv'
    output_file = os.path.join(output_dir, dynamic_filename)
    df .to_csv(output_file, index=False)
    
    
    
    
        
