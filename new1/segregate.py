import boto3
import sys
import botocore
import logging
from awsglue.utils import getResolvedOptions
from botocore.exceptions import NoCredentialsError
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

def list_files_in_s3_bucket(bucket_name, prefix):
    s3 = boto3.client('s3')
    file_keys = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                file_keys.extend(obj['Key'] for obj in page['Contents'] if obj['Key'] != prefix)
        if not file_keys:
            logging.info(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error listing files in bucket '{bucket_name}' with prefix '{prefix}': {e}")
    return file_keys

def read_file_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        logging.info(f"Attempting to read file '{file_key}' from bucket '{bucket_name}'...")
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = response['Body']
        content = body.read().decode('utf-8')
        logging.info(f"Successfully read content from '{file_key}'.")
        return content.splitlines()
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logging.error(f"Error: The file with key '{file_key}' does not exist in bucket '{bucket_name}'.")
        else:
            logging.error(f"Unexpected error: {e}")
        return None

def segregate_records(lines):
    records = {}
    footer_info = {}
    for line in lines:
        fields = line.strip().split('|')
        if len(fields) < 2:
            continue
        record_type = fields[0]
        if record_type == 'T':  # Assuming footer record type is 'T'
            footer_info = parse_footer(fields)
        elif record_type not in ('H', 'T'):
            records.setdefault(record_type, []).append(line)
    return records, footer_info

def parse_footer(fields):
    footer_info = {}
    for field in fields[1:]:
        key, value = field.split('~')
        footer_info[key] = int(value)
    return footer_info

def write_segregated_files(records, footer_info, output_prefix):
    for record_type, lines in records.items():
        total_records = footer_info.get(record_type, 0)
        file_name = f"rec_type_{record_type}.txt"
        output_path = f"{output_prefix}/{file_name}"
        with open(output_path, 'w') as file:
            for line in lines:
                file.write(line.strip() + f"|{total_records}\n")
        logging.info(f"Wrote segregated file to {output_path}")

def upload_files_to_s3(bucket_name, prefix, record_types):
    s3 = boto3.client('s3')
    for record_type in record_types:
        file_name = f"rec_type_{record_type}.txt"
        file_key = f"{prefix}/{file_name}"
        try:
            with open(file_name, 'rb') as file:
                logging.info(f"Uploading file '{file_key}' to bucket '{bucket_name}'...")
                s3.upload_fileobj(file, bucket_name, file_key)
                logging.info(f"Successfully uploaded file '{file_key}' to bucket '{bucket_name}'.")
        except Exception as e:
            logging.error(f"Failed to upload file '{file_key}' to bucket '{bucket_name}': {e}")

def read_s3_json(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        logging.error(f"Error reading JSON from S3: {e}")
        return None

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_data_bkt', 'config_file', 'env'])
src_data_bkt = args['src_data_bkt']
config_file = args['config_file']

# Get current date for folder structure
current_date = datetime.now().strftime("%Y/%m/%d/%H")
output_prefix = f"Transpose_output/rec_type_9005/{current_date}"

# Read configuration
bkt_params = read_s3_json(src_data_bkt, config_file)

# Define your bucket names and prefixes
input_bucket_name = bkt_params[args['env']]['RAW_DATA_BKT']
input_prefix = 'batch_landing/'

# List all files in the input bucket with the specified prefix
file_keys = list_files_in_s3_bucket(input_bucket_name, input_prefix)

if file_keys:
    for file_key in file_keys:
        # Read the file from S3
        lines = read_file_from_s3(input_bucket_name, file_key)
        if lines:
            # Segregate records
            records, footer_info = segregate_records(lines)

            # Write segregated files locally
            write_segregated_files(records, footer_info, output_prefix)

            # Upload segregated files to S3
            upload_files_to_s3(bkt_params[args['env']]['SPLITTED_DATA_BKT'], output_prefix, records.keys())
else:
    logging.info("No files to process.")
