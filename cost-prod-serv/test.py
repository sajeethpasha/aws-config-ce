import pandas as pd
from io import StringIO

data = """product,account,account_id,service,cost,total_product_cost,start_date,end_date
edgesfui,aws-green R&D,767904627276,AWS Shield,0.0156901578,116.5820155407,2024-06-01,2024-06-30
edgesfui,aws-green R&D,767904627276,Amazon Elastic Container Service,0,116.5820155407,2024-06-01,2024-06-30
edgesfui,aws-green R&D,767904627276,Amazon Elastic Load Balancing,116.5663253829,116.5820155407,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-green R&D,767904627276,AWS AppSync,2.06922,4618.5047295657,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-green R&D,767904627276,AWS Lambda,4613.173888262,4618.5047295657,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-green R&D,767904627276,Amazon API Gateway,0.0000739883,4618.5047295657,2024-06-01,2024-06-30
scdvisualizerbeta,aws-red,565378680304,AWS Shield,0.0048262733,140.3131135641,2024-06-01,2024-06-30
scdvisualizerbeta,aws-red,565378680304,Amazon ElastiCache,0,140.3131135641,2024-06-01,2024-06-30
scdvisualizerbeta,aws-red,565378680304,Amazon Elastic Load Balancing,25.8436832072,140.3131135641,2024-06-01,2024-06-30
scdvisualizerbeta,aws-red,565378680304,Amazon Relational Database Service,114.4646040836,140.3131135641,2024-06-01,2024-06-30
studyfeasibility,aws-red,565378680304,Amazon ElastiCache,9638.433,9638.433,2024-06-01,2024-06-30"""

# Read data into a DataFrame
df = pd.read_csv(StringIO(data))

# Create a product DataFrame
product_df = df[['product', 'account', 'account_id', 'total_product_cost', 'start_date', 'end_date']].drop_duplicates()

# Create a service DataFrame
service_df = df[['product', 'account_id', 'service', 'cost']]

# Print the DataFrames
print("Product DataFrame:")
print(product_df)

print("\nService DataFrame:")
print(service_df)
