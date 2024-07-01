import pandas as pd

csv_file_path = 'D:\\supports\\akhil\\orange_data\\testing' 

# csv_file_path='map.csv'
df = pd.read_csv(csv_file_path)
# Load CSV data into a DataFrame
# df = pd.read_csv('D:\supports\akhil\orange_data\testing\Map-orange-cost- data.csv')

# Filter rows where 'cost_category' contains the specified substring
filtered_df = df[df['cost_category'].str.contains('"product_tier_1":"Intelligent Trials"')]

# Save the filtered data to a new CSV file
filtered_df.to_csv('filtered_data.csv', index=False)


selected_columns = ['line_item_product_code', 'line_item_resource_id', 'line_item_unblended_cost', 'cost_category']
filtered_and_selected_df = filtered_df[selected_columns]

# Save the final result to a new CSV file
filtered_and_selected_df.to_csv('filtered_and_selected_data.csv', index=False)

