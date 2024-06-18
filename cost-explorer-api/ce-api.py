import boto3

# Initialize a session using Amazon Cost Explorer
ce = boto3.client('ce')

# Define the parameters for the request
response = ce.get_cost_and_usage_with_resources(
    TimePeriod={
        'Start': '2024-05-01',
        'End': '2024-05-10'
    },
    Granularity='MONTHLY',
    Metrics=['BlendedCost', 'UnblendedCost', 'UsageQuantity'],
    GroupBy=[
        {
            'Type': 'DIMENSION',
            'Key': 'SERVICE'
        },
    ],
    Filter={
        'Dimensions': {
            'Key': 'SERVICE',
             'Values': ['Amazon Simple Storage Service']
        }
    }
)

# Print the response
print(response)
