import boto3


# Create a Cost Explorer client
ce_client = boto3.client('ce')

# Define the time period for the query
time_period = {
    'Start': '2024-05-01',  # Start date for fetching data (YYYY-MM-DD)
    'End': '2024-05-05'     # End date for fetching data (YYYY-MM-DD)
}

# Define the filter and metrics for the query
query = {
    'TimePeriod': time_period,
    'Granularity': 'MONTHLY',  # Can be 'DAILY', 'MONTHLY', or 'HOURLY'
    'Filter': {
        'Dimensions': {
            'Key': 'SERVICE',
            'Values': ['Amazon Simple Storage Service']
        }
    },
    'Metrics': ['UnblendedCost']
}

# Make the request
response = ce_client.get_cost_and_usage_with_resources(**query)

# Print the response
print(response)
