import pandas as pd
import pandas as pd
import json

# Assuming your data is stored in a variable named 'x'

# Path to your local JSON file (replace with your actual path)
file_path = "awsfilejson/green-monthly-byproduct.json"

try:
  # Open the JSON file and read its content
  with open(file_path, 'r') as f:
    data = json.load(f)
except FileNotFoundError:
  print(f"Error: JSON file not found at {file_path}")
  exit(1)

# Proceed with processing the data as before (assuming 'x' is now 'data')
results = data.get('ResultsByTime', [{}])[0]

# Get the first element of ResultsByTime (assuming there's data)
# results = x.get('ResultsByTime', [{}])[0]

# Extract Groups data
groups = results.get('Groups', [])

# Check if there are any Groups
if not groups:
  print("No Groups data found in the provided response.")
else:
  # Create an empty list to store group data as dictionaries
  group_data = []

  # Loop through each group and extract relevant data
  for group in groups:
    keys = group.get('Keys', [])
    metrics = group.get('Metrics', {})
    # Assuming you want UnblendedCost's Amount
    cost = metrics.get('UnblendedCost', {}).get('Amount', 0)

    # Create a dictionary for each group
    group_dict = {
        "Service": keys[0],  # Assuming first key is service name
        "Product": keys[1].split("$")[1],  # Extract product name after "$"
        "Cost (USD)": cost
    }
    group_data.append(group_dict)

  # Create the DataFrame from the list of dictionaries
  df = pd.DataFrame(group_data)

  # Print or use the DataFrame as needed
  print(df.to_string())
  
  
  # Define the output CSV file path (replace with your desired name)
  csv_file_path = "output.csv"

  # Save the DataFrame to a CSV file
  df.to_csv(csv_file_path, index=False)
  