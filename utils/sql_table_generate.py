import datetime
import pandas as pd
import os
import boto3
import io
from io import StringIO
import gzip



def create_table_query(df, table_name):
    # Mapping from pandas dtype to SQL type
    dtype_mapping = {
        'int64': 'INTEGER',
        'float64': 'double precision',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'timedelta64[ns]': 'INTERVAL'
    }

    # Start the CREATE TABLE SQL statement
    create_table_sql = f"CREATE TABLE {table_name} (\n"
    
    # Loop over columns in the DataFrame to create column definitions
    for col in df.columns:
        col_name = col
        col_type = str(df[col].dtype)
        sql_type = dtype_mapping.get(col_type, 'TEXT')  # Default to TEXT if type is not found
        
        create_table_sql += f"    {col_name} {sql_type},\n"
    
    # Remove the last comma and add closing parenthesis
    create_table_sql = create_table_sql.rstrip(',\n') + "\n);"
    
    return create_table_sql