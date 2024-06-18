import argparse
import boto3
import json
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError

account_map = {
    'orange': '016266444216',
    'red': '016266444216',
    'green': '016266444216'
}

def get_costs(account, granularity, group_by):
    dollar_formatter = lambda amount: f"${amount:,.2f}"

    today = datetime.now()
    first_day = (today - timedelta(days=14)).strftime('%Y-%m-%d')
    last_day = today.strftime('%Y-%m-%d')
    
    try:
        client = boto3.client('ce', region_name='us-east-1')
    except NoCredentialsError:
        print("Credentials not available")
        return

    filter = None
    if account is not None:
        filter = {
            'Dimensions': {
                'Key': 'LINKED_ACCOUNT',
                'Values': [account_map[account]]
            }
        }

    if group_by == 'resource':
        group_by = [{'Type': 'DIMENSION', 'Key': 'SERVICE'}, {'Type': 'DIMENSION', 'Key': 'RESOURCE_ID'}]
    elif group_by == 'product':
        group_by = [{'Type': 'DIMENSION', 'Key': 'SERVICE'}, {'Type': 'TAG', 'Key': 'Product'}]
    else:
        group_by = [{'Type': 'DIMENSION', 'Key': 'SERVICE'}]

    try:
        results = client.get_cost_and_usage(
            TimePeriod={'Start': first_day, 'End': last_day},
            Granularity=granularity.upper(),
            Metrics=['UnblendedCost'],
            GroupBy=group_by,
            Filter=filter
        )
    except client.exceptions.DataUnavailableException:
        print("Data not available")
        return

    table = []

    for by_time in results['ResultsByTime']:
        month = datetime.strptime(by_time['TimePeriod']['End'], '%Y-%m-%d').strftime('%B')
        for group in by_time['Groups']:
            service_name = group['Keys'][0]
            existing = next((item for item in table if item['service'] == service_name), None)
            if existing is None:
                table.append({
                    'service': service_name,
                    month: dollar_formatter(float(group['Metrics']['UnblendedCost']['Amount']))
                })
            else:
                existing[month] = dollar_formatter(float(group['Metrics']['UnblendedCost']['Amount']))

    # print(json.dumps(table, indent=4))
    with open(f'output/{account}-{granularity}-by{group_by}.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

def main():
    parser = argparse.ArgumentParser(description='View cost data')
    parser.add_argument('-a', '--account', choices=['orange', 'green', 'red'], help='an AWS account')
    parser.add_argument('-g', '--granularity', choices=['hourly', 'daily', 'monthly'], default='monthly', help='the granularity of costs')
    parser.add_argument('--groupby', choices=['product', 'resource'], default='resource', help='how to group the results')
    args = parser.parse_args()

    get_costs(account=args.account, granularity=args.granularity, group_by=args.groupby)

if __name__ == '__main__':
    main()
