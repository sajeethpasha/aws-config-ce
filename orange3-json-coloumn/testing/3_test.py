import pandas as pd
import json
import pandas as pd
import json
import pandas as pd
import json
from datetime import datetime
import os
import boto3
import io
from io import StringIO
import gzip


# Define the data
data = [
    {
        "cost_category": {"portfolio": "AWS Charge Types", "production_and_development": "Production", "a_w_s_charge_types": "AWS Charge Types", "product_group": "Medidata AI", "development": "Development", "product_tier_1": "AWS Charge Types"},
        "product": {"product_name": "Amazon Relational Database Service"},
        "resource_tags": {}
    },
    {
        "cost_category": {"production_and_development": "Production", "product_group": "Medidata AI", "development": "Development", "product_tier_1": "Central"},
        "product": {"transfer_type": "IntraRegion-xAZ-In", "product_name": "Amazon Elastic Compute Cloud", "region": "us-east-1", "servicename": "AWS Data Transfer"},
        "resource_tags": {"user_product": "Aviatrix", "aws_created_by": "AssumedRole:AROA4EKNR3SHMCYJC5VGU:AssumeRoleSession", "user_type": "gateway", "user_environment": "Production", "user_name": "aviatrix-use1-orange-m8o-pr-spk-gw"}
    },
    {
        "cost_category": {"production_and_development": "Production", "product_group": "Medidata AI", "development": "Development"},
        "product": {"transfer_type": "IntraRegion-xAZ-In", "product_name": "Amazon FSx", "region": "us-east-1", "servicename": "AWS Data Transfer"},
        "resource_tags": {"user_name": "Data-transfer-to-Prod"}
    },
    {
        "cost_category": {"portfolio": "AWS Shared Services", "a_w_s_shared_services": "AWS Shared Services", "production_and_development": "Production", "product_group": "Medidata AI", "development": "Development", "product_tier_1": "AWS Shared Services"},
        "product": {"transfer_type": "IntraRegion-xAZ-In", "product_name": "Amazon Virtual Private Cloud", "region": "us-east-1", "servicename": "AWS Data Transfer"},
        "resource_tags": {}
    },
    {
        "cost_category": {"production_and_development": "Production", "product_group": "Medidata AI", "development": "Development"},
        "product": {"transfer_type": "InterRegion Outbound", "product_name": "Amazon Simple Storage Service", "region": "us-east-2", "servicename": "AWS Data Transfer"},
        "resource_tags": {"aws_created_by": "AssumedRole:AROA4EKNR3SHIZIN4EA7U:SageMaker"}
    },
    {
        "cost_category": {"production_and_development": "Production", "product_group": "Medidata AI", "development": "Development"},
        "product": {"transfer_type": "InterRegion Outbound", "product_name": "Amazon Simple Storage Service", "region": "us-east-2", "servicename": "AWS Data Transfer"},
        "resource_tags": {"aws_created_by": "AssumedRole:AROAJMKIJFZ6WQV4FEZV6:rkrishnan@mdsol.com"}
    }
]


# Normalize JSON data
normalized_data = []


for entry in data:
    combined = {
        **{f'cc_{k}': v for k, v in entry['cost_category'].items()},
        **{f'product_{k}': v for k, v in entry['product'].items()},
        **{f'rt_{k}': v for k, v in entry['resource_tags'].items()}
    }
    normalized_data.append(combined)

# Convert to DataFrame
df = pd.DataFrame(normalized_data)

# Add original JSON columns for reference if needed
df['cost_category'] = [json.dumps(entry['cost_category']) for entry in data]
df['product'] = [json.dumps(entry['product']) for entry in data]
df['resource_tags'] = [json.dumps(entry['resource_tags']) for entry in data]


# Generate the dynamic filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"col-json-test{timestamp}.csv"

# Save the filtered DataFrame to a new CSV file with the dynamic filename
df.to_csv(fr'D:\supports\akhil\orange_data\testing\out\col-json\{output_filename}', index=False)

print(df)

