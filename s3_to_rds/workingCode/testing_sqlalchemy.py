import os
import boto3
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Database connection details
db_host = "database-1.crwpj4y0tolc.ap-south-1.rds.amazonaws.com"
db_name = "postgres"
db_user = "postgres"
db_password = "Sajeeth123"
db_port = "5432"

# Create a connection to the PostgreSQL database
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}')


# cur.execute("""
#     CREATE TABLE IF NOT EXISTS your_table_name (
#         ID INT,
#         Name VARCHAR(255),
#         Age INT
#     )
# """)


# Example DataFrame
data = {
    'ID': [1, 2, 3, 4, 5,6],
    'Name': ['John', 'Alice', 'dd', 'Carol', 'hello','sajeeth'],
    'Age': [25, 30, 35, 40, 45,78]
}

df = pd.DataFrame(data)
# Upload DataFrame to PostgreSQL
df.to_sql('your_table_name', con=engine, if_exists='append', index=False)

print("DataFrame uploaded successfully.")
