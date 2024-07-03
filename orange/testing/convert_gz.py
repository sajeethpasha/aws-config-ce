import pandas as pd
import gzip
import io

def load_csv_and_compress(input_filepath, output_filepath):
    # Load CSV file into a DataFrame
    df = pd.read_csv(input_filepath)
    print("CSV file loaded...")

    # Convert DataFrame to CSV bytes
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Compress CSV content using gzip
    with gzip.open(output_filepath, 'wt') as gz_file:
        gz_file.write(csv_content)
    
    print(f"CSV file compressed and saved to {output_filepath}")

if __name__ == "__main__":
    input_filepath = r'D:\temp\orange\convert\orange_data_zip_file.csv'  # Replace with your input CSV file path
    output_filepath = r'D:\temp\orange\convert\orange_data_zip_file.csv.gz'  # Replace with your desired output gz file path

    load_csv_and_compress(input_filepath, output_filepath)
