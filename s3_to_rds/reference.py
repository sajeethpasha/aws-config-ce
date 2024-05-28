import boto3
import psycopg2
from psycopg2 import sql

# AWS credentials and region
aws_access_key_id = 'YOUR_ACCESS_KEY_ID'
aws_secret_access_key = 'YOUR_SECRET_ACCESS_KEY'
region_name = 'YOUR_REGION_NAME'

# Initialize Boto3 clients for S3 and RDS
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
rds_host = 'YOUR_RDS_ENDPOINT'
rds_port = '5432'
rds_dbname = 'postgresqldb'
rds_user = 'postgres'
rds_password = 'Sajeeth123'

# Connect to RDS database
conn = psycopg2.connect(host=rds_host, port=rds_port, database=rds_dbname, user=rds_user, password=rds_password)
cur = conn.cursor()

# S3 bucket and object key
bucket_name = 'YOUR_S3_BUCKET_NAME'
object_key = 'YOUR_S3_OBJECT_KEY'

# Download data from S3
response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
data = response['Body'].read().decode('utf-8')

# Parse data and insert into RDS
for line in data.split('\n'):
    # Assuming data is in CSV format, modify accordingly if it's in a different format
    values = line.split(',')
    # Assuming table_name and columns are known
    insert_query = sql.SQL("INSERT INTO table_name (column1, column2, ...) VALUES (%s, %s, ...)").format(sql.Identifier('column1'), sql.Identifier('column2'), ...)
    cur.execute(insert_query, values)
    conn.commit()

# Close database connection
cur.close()
conn.close()
