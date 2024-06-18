import boto3
import csv
import psycopg2
import io
import pandas as pd
from datetime import datetime
import json
from botocore.exceptions import ClientError


def get_secret():
    db_host = 'database-1.crwpj4y0tolc.ap-south-1.rds.amazonaws.com'
    db_port = '5432'
    db_user = 'postgres'
    db_password = 'Sajeeth123'
    db_name = 'postgres'

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    return conn   
    # return conn

def get_latest_files(bucket_name, folder_prefix, max_keys):
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder_prefix,
        MaxKeys=max_keys
    )
    files = [(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])]
    while 'NextContinuationToken' in response:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_prefix,
            MaxKeys=max_keys,
            ContinuationToken=response['NextContinuationToken']
        )
        files.extend([(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])])
    files = [file for file in files if file[0] != folder_prefix]
    sorted_files = sorted(files, key=lambda x: x[1], reverse=True)
    latest_files = [file[0] for file in sorted_files]
    print(latest_files)
    return latest_files


def read_file_to_dataframe(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj['Body'])
    # df_modified = df[1:]
    # df_modified = remove_char_from_colnames(df_modified, '($)')

    # return df_modified
    # df.columns = [replace_special_chars_with_space(col) for col in df.columns]
    print("read_file_to_dataframe complited.....")
    return df


# Remove the character '($)' from column names
def remove_char_from_colnames(df, char_to_remove):
    """Removes a specific character from all column names in a DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to modify.
        char_to_remove (str): The character to remove from column names.

    Returns:
        pandas.DataFrame: The DataFrame with modified column names.
    """

    df.columns = df.columns.str.replace(char_to_remove, '', regex=True)
    return df


def process_products(products_latest_files):
    dfs = []
    for file in products_latest_files:
        print("file :{}",file)
        df = read_file_to_dataframe(bucket_name, file)
        print(df)
        print("df columns list:" + "columns" + df.columns)
        df = updteDf(df, 'Product')
        file_name = file.split('/')[-1]
        source = file_name.split('-')[0]
        df['Source'] = [source] * len(df)
        print("df array adding.....{}", len(df))
        dfs.append(df)
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df.insert(0, 'id', range(1, len(merged_df) + 1))
    merged_df.fillna(0.0, inplace=True)
    renameProductDf(merged_df)
    print('coloumns names only Product: {}', list(merged_df.columns))
    return merged_df


def process_services(services_latest_files):
    dfs = []
    for file in services_latest_files:
        print("file :{}",file)
        df = read_file_to_dataframe(bucket_name, file)
        df = updteDf(df, 'Service')
        file_name = file.split('/')[-1]
        source = file_name.split('-')[0]
        df['Source'] = [source] * len(df)
        dfs.append(df)
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df.insert(0, 'id', range(1, len(merged_df) + 1))
    merged_df.fillna(0.0, inplace=True)
    renameServiceDf(merged_df)
    print('coloumns names only service : {}',  list(merged_df.columns))
    return merged_df


def create_table_with_schema(cursor):
    try:
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS test_products (
                                    id serial primary key,
                                    Date date,
                                    "performsfapi($)" double precision,
                                    "intelligent-trials-capabilities($)" double precision,
                                    "intelligenttrialscapabilities($)" double precision,
                                    "intelligenttrialsprecompute($)" double precision,
                                    "intelligenttrialsapi($)" double precision,
                                    "edgesfui($)" double precision,
                                    "Performance Analytics($)" double precision,
                                    "Intelligent Trials - Intelligent Trials Capabilities($)" double precision,
                                    "Intelligent Trials - Admin($)" double precision,
                                    "Intelligent Trials Capabilities($)" double precision,
                                    "Total costs($)" double precision,
                                    "Source" TEXT)
                    """)
        cursor.execute("""
          CREATE TABLE IF NOT EXISTS test_services (  id serial primary key,
                            Date date,
                            "ElastiCache($)" double precision,
                            "Lambda($)" double precision,
                            "Relational Database Service($)" double precision,
                            "Elastic Load Balancing($)" double precision,
                            "EC2-Instances($)" double precision,
                            "EC2-Other($)" double precision,
                            "S3($)" double precision,
                            "SQS($)" double precision,
                            "Inspector($)" double precision,
                            "AppSync($)" double precision,
                            "Shield($)" double precision,
                            "API Gateway($)" double precision,
                            "Elastic Container Service($)" INTEGER,
                            "Total costs($)" double precision,
                            "Source" TEXT)
                    """)
        print("Tables are created successfully")
        cursor.connection.commit()
    except Exception as e:
        print("Error creating tables:", e)


def insert_data_into_postgresql(cursor, data, table_name):
    try:
        for index, row in data.iterrows():
            # columns = ','.join(data.columns)
            columns = ','.join('"'+data.columns+'"')
            placeholders = ','.join(['%s'] * len(row))
            conflict_columns = 'id'
            # conflict_update = ','.join([f"{col} = excluded.{col}" for col in row.keys()])
            new_col=[]
            for col in row.keys():
                ncl='"'+col+'"'
                new_col.append(f"{ncl} = excluded.{ncl}")
                conflict_update=','.join(new_col)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict_update}"
            cursor.execute(query, tuple(row))
        cursor.connection.commit()
        print("Data inserted into Postgresql successfully")
    except Exception as e:
        print("Error inserting data in PostgreSQl:", e)


def validate(date_text):
    print("date_text: {}",  date_text)
    try:
        datetime.strptime(str(date_text), '%Y-%m-%d')
        return True
    except ValueError:
        return False


def updteDf(df, clm):
    print("inside the updteDf.....")

    condition = df[clm].apply(validate)
    print("updteDf complited...")
    return df[condition]


def renameProductDf(df):
    df.rename(columns={'Product':'Date'},
              inplace=True, errors='raise')


def renameServiceDf(df):
    df.rename(columns={'Service':'Date'},
              inplace=True, errors='raise')


if __name__ == "__main__":
    try:
        print('starting code 2')
        s3_client = boto3.client('s3')
        bucket_name = 'product-service-test'

        folder_prefix = 'product/'
        # bucket_name = 'product-service'
        # folder_prefix = 'product/'
        products_latest_files = get_latest_files(bucket_name, folder_prefix, 2)
        print("products_latest_files detaisl fetched......")

        products_data = process_products(products_latest_files)
        print("products_data fetched.....")

        print("write dataframe to S3 location")
        # products_data./to_csv("s3://sp-bucket-123/samples/products_data.csv", header=True, index=False)

        folder_prefix = 'services/'
        # folder_prefix = 'services/'
        services_latest_files = get_latest_files(bucket_name, folder_prefix, 3)
        services_data = process_services(services_latest_files)
        # services_data.to_csv("s3://sp-bucket-123/samples/services_data.csv", header=True, index=False)

        conn = get_secret()

        print("Before cursor connection establish")
        cursor = conn.cursor()
        print("Cursor connection established")
        # create_table_with_schema(cursor)
        insert_data_into_postgresql(cursor, products_data, 'test_products')
        insert_data_into_postgresql(cursor, services_data, 'test_services')
        cursor.close()
        conn.close
        print('starting end.')
    except Exception as e:
        print(f"Error in code: {e}")