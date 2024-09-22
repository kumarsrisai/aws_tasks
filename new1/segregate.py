import sys
import json
import logging
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Initialize S3 client
s3 = boto3.client('s3')

# Function to read S3 JSON configuration file
def read_s3_json(bucket_name, key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except NoCredentialsError:
        logging.error("AWS credentials not found.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error reading JSON from S3: {str(e)}")
        sys.exit(1)

# Function to list files in an S3 bucket with a given prefix
def list_files_in_s3_bucket(bucket_name, prefix):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            return [content['Key'] for content in response['Contents']]
        return []
    except Exception as e:
        logging.error(f"Error listing files in S3: {str(e)}")
        sys.exit(1)

# Function to read a file from S3
def read_file_from_s3(bucket_name, key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8').splitlines()
        return content
    except Exception as e:
        logging.error(f"Error reading file from S3: {str(e)}")
        return None

# Function to segregate records based on record type
def segregate_records(lines):
    records = {}
    for line in lines:
        fields = line.strip().split('|')
        record_type = fields[0]
        if record_type not in ('H', 'T'):  # Ignore header and footer
            if record_type not in records:
                records[record_type] = []
            records[record_type].append(line)
    return records

# Function to write segregated files to S3
def write_segregated_files(records, output_prefix, output_bucket_name):
    try:
        for record_type, lines in records.items():
            file_key = f"{output_prefix}/rec_type_{record_type}.txt"
            body = '\n'.join(lines)

            # Upload the segregated record type file to S3
            s3.put_object(Bucket=output_bucket_name, Key=file_key, Body=body)
            logging.info(f"Uploaded file '{file_key}' to bucket '{output_bucket_name}'")

    except Exception as e:
        logging.error(f"Error writing files to S3: {str(e)}")

def main(args):
    # Extract environment from arguments
    env = args.get('env')

    # Source bucket and config file provided as arguments
    src_data_bkt = args['src_data_bkt']
    config_file = args['config_file']

    # Load bucket configuration from the JSON file
    bkt_params = read_s3_json(src_data_bkt, config_file)

    # Set input and output bucket names based on the environment
    input_bucket_name = bkt_params[env]['RAW_DATA_BKT']  # Raw data bucket (e.g., ddsl-raw-developer)
    output_bucket_name = bkt_params[env]['SPLITTED_DATA_BKT']  # Splitted data bucket (e.g., ddsl-raw-extended-developer)

    # Prefix for input files (where raw data is located)
    input_prefix = 'batch_landing/'

    # Get current date for folder structure dynamically
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_prefix = f"batch_splitted/accounting/dt={current_date}"  # Output folder based on current date

    # List all files in the input bucket with the specified prefix
    file_keys = list_files_in_s3_bucket(input_bucket_name, input_prefix)

    # Process the files if any exist
    if file_keys:
        for file_key in file_keys:
            # Read the file from S3
            lines = read_file_from_s3(input_bucket_name, file_key)
            if lines:
                # Segregate records
                records = segregate_records(lines)

                # Write segregated files and upload to S3
                write_segregated_files(records, output_prefix, output_bucket_name)
    else:
        logging.info("No files to process.")

if __name__ == "__main__":
    # Example usage: pass arguments when invoking this script
    args = {
        'env': 'dev',  # Environment, e.g., dev
        'src_data_bkt': 'ddsl-raw-developer',  # Source bucket name for configuration
        'config_file': 'job_config/bucket_config.json'  # Config file location in the source bucket
    }
    main(args)
