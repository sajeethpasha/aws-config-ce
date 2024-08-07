import datetime
import pandas as pd
import os
import boto3
import io
import psycopg2

def process_data(file_path, output_dir, bucket, out_file):
    def get_df_from_s3_csv(bucket, filepath):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=filepath)
        csv_content = response['Body'].read().decode('utf-8')  # Decode bytes to string
        return pd.read_csv(io.StringIO(csv_content))

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

    def create_table_with_schema(cursor):
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monlty_report (
                    id SERIAL PRIMARY KEY,
                    product TEXT,
                    account TEXT,
                    account_id TEXT,
                    service TEXT,
                    cost double precision,
                    total_product_cost double precision,
                    date DATE,
                    CONSTRAINT unique_record UNIQUE (product, account, account_id, service, date)
                );
            """)
            print("Table 'monlty_report' created successfully")
            cursor.connection.commit()
        except Exception as e:
            print("Error creating table:", e)

    def insert_data_into_postgresql(cursor, data, table_name):
        try:
            for index, row in data.iterrows():
                columns = ','.join(data.columns)
                placeholders = ','.join(['%s'] * len(row))
                conflict_columns = ['product', 'account', 'account_id', 'service', 'date']  # Assuming these columns define uniqueness
                conflict_update = ','.join([f"{col} = excluded.{col}" for col in data.columns])
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {conflict_update}"
                cursor.execute(query, tuple(row))
            cursor.connection.commit()
            print("Data inserted into PostgreSQL successfully")
        except Exception as e:
            print("Error inserting data into PostgreSQL:", e)

    try:
        df = get_df_from_s3_csv(bucket, file_path)
        df['date'] = pd.to_datetime(df['start_date']).dt.date
        df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df = df.drop(columns=['start_date', 'end_date'])

        current_datetime = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        dynamic_filename = f'update_date_{current_datetime}.csv'
        output_file_path = os.path.join(out_file, dynamic_filename)

        if not os.path.exists(out_file):
            os.makedirs(out_file)

        df.to_csv(output_file_path, index=False)

        conn = get_secret()
        print("Before cursor connection established")
        cursor = conn.cursor()
        print("Cursor connection established")

        create_table_with_schema(cursor)
        insert_data_into_postgresql(cursor, df, "monlty_report")

        cursor.close()
        conn.close()
        print('Data processing completed successfully.')

    except Exception as e:
        print(f"Error in processing data: {e}")

if __name__ == "__main__":
    file_path = r'raw-data/Monthly_costs.csv'
    output_dir = r'out-files/'
    bucket = 'month-sample'
    out_file = r'D:\supports\akhil\temp\out'

    print('Starting data processing...')
    process_data(file_path, output_dir, bucket, out_file)
