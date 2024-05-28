import pandas as pd
import boto3
import psycopg2
from io import StringIO

# Step 1: Create a DataFrame
data = {
    'ID': [1, 2, 3, 4, 5],
    'Name': ['John', 'Alice', 'Bob', 'Carol', 'David'],
    'Age': [25, 30, 35, 40, 45]
}
df = pd.DataFrame(data)

# Step 2: Convert DataFrame to CSV format
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
csv_data = csv_buffer.getvalue()

# Step 3: Connect to AWS RDS instance
rds_client = boto3.client('rds')
# Retrieve RDS instance details
response = rds_client.describe_db_instances(DBInstanceIdentifier='your-db-instance-id')
# Extract database endpoint and credentials
endpoint = response['DBInstances'][0]['Endpoint']['Address']
username = 'your-username'
password = 'your-password'
dbname = 'your-db-name'

# Step 4: Establish connection to RDS instance using psycopg2

conn = psycopg2.connect(
    dbname=dbname,
    user=username,
    password=password,
    host=endpoint,
    port=5432
)


cur = conn.cursor()
cur.execute("""
        id SERIAL PRIMARY KEY
        create table query
        CREATE TABLE "confcost" (
        "tags" text,
        "configurationItemVersion" double precision,
        "configurationItemCaptureTime" text,
        "configurationItemCaptureDate" text,
        "configurationStateId" bigint,
        "awsAccountId" bigint,
        "configurationItemStatus" text,
        "resourceType" text,
        "resourceId" text,
        "awsRegion" text,
        "identity_time_interval" text NULL,
        "bill_billing_period_start_date" text NULL,
        "bill_billing_period_end_date" text NULL,
        "line_item_usage_start_date" text NULL,
        "line_item_usage_end_date" text NULL,
        "line_item_product_code" text NULL,
        "line_item_operation" text NULL,
        "line_item_resource_id" text NULL,
        "line_item_unblended_rate" text NULL,
        "line_item_unblended_cost" text NULL,
        "line_item_blended_rate" text NULL,
        "line_item_blended_cost" text NULL,
        "line_item_usage_amount" text NULL
        );
""")
conn.commit()

# Load data from CSV into the table
cur.copy_from(StringIO(csv_data), 'your_table_name', sep=',', columns=('ID', 'Name', 'Age'))
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()