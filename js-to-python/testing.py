import argparse
import json
import os
from datetime import datetime, timedelta
import boto3

# Mapping of account names to AWS account IDs
account_map = {
    'orange': '016266444216',
    'red': '016266444216',
    'green': '016266444216'
}

def get_costs(account, granularity, group_by):
    # Initialize the AWS Cost Explorer client
    client = boto3.client('ce', region_name='ap-south-1')

    # Define time period (last 14 days from today)
    today = datetime.now()
    first_day = (today - timedelta(days=14)).strftime('%Y-%m-%d')
    last_day = today.strftime('%Y-%m-%d')

    # Set up filter by account if specified
    filter = None
    if account is not None:
        filter = {
            'Dimensions': {
                'Key': 'LINKED_ACCOUNT',
                'Values': [account_map[account]]
            }
        }

    # Determine command based on group_by option
    command_args = {
        'TimePeriod': {
            'Start': first_day,
            'End': last_day
        },
        'Granularity': granularity.upper(),
        'Metrics': ['UnblendedCost'],
        'GroupBy': [{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
    }

    if group_by == 'resource':
        command_args['GroupBy'].append({'Type': 'DIMENSION', 'Key': 'RESOURCE_ID'})
    elif group_by == 'product':
        command_args['GroupBy'].append({'Type': 'TAG', 'Key': 'Product'})

    if filter:
        command_args['Filter'] = filter

    # Execute the appropriate AWS Cost Explorer API command
    if group_by == 'resource':
        response = client.get_cost_and_usage_with_resources(**command_args)
    else:
        response = client.get_cost_and_usage(**command_args)

    # Process the response to build the output
    table = {}
    for time_period in response['ResultsByTime']:
        month = datetime.strptime(time_period['TimePeriod']['End'], '%Y-%m-%d').strftime('%B')
        for group in time_period['Groups']:
            service_name = group['Keys'][0]
            cost = group['Metrics']['UnblendedCost']['Amount']
            if service_name not in table:
                table[service_name] = {}
            table[service_name][month] = cost

    # Write the output to a JSON file
    output_filename = f"output/{account}-{granularity}-by{group_by}.json"
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    with open(output_filename, 'w') as outfile:
        json.dump(response, outfile, indent=4)

def main():
    parser = argparse.ArgumentParser(description='View AWS cost data')
    parser.add_argument('-a', '--account', choices=['orange', 'green', 'red'], help='an AWS account')
    parser.add_argument('-g', '--granularity', choices=['hourly', 'daily', 'monthly'], default='monthly', help='the granularity of costs')
    parser.add_argument('--groupby', choices=['product', 'resource'], default='resource', help='how to group the results')

    args = parser.parse_args()
    get_costs(args.account, args.granularity, args.groupby)

if __name__ == '__main__':
    main()
