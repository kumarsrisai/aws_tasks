import boto3
import sys
import botocore
import logging
from awsglue.utils import getResolvedOptions
from botocore.exceptions import NoCredentialsError
import json
import boto3
from urllib.parse import urlparse


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

def write_segregated_files(records, footer_info):
    for record_type, lines in records.items():
        total_records = footer_info.get(record_type, 0)
        file_name = f"rec_type_{record_type}.txt"  # Updated filename format
        with open(file_name, 'w') as file:
            for line in lines:
                file.write(line.strip() + f"|{total_records}\n")

def upload_files_to_s3(bucket_name, prefix, record_types):
    s3 = boto3.client('s3')
    for record_type in record_types:
        file_name = f"rec_type_{record_type}.txt"  # Updated filename format
        file_key = f"{prefix}/{file_name}"
        try:
            with open(file_name, 'rb') as file:
                logging.info(f"Uploading file '{file_key}' to bucket '{bucket_name}'...")
                s3.upload_fileobj(file, bucket_name, file_key)
                logging.info(f"Successfully uploaded file '{file_key}' to bucket '{bucket_name}'.")
        except Exception as e:
            logging.error(f"Failed to upload file '{file_key}' to bucket '{bucket_name}': {e}")

args = getResolvedOptions(sys.argv,['env'])
env = args['env']

# Function to read JSON file from S3
def read_s3_json(bucket, key):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return json.loads(data)
    except NoCredentialsError:
        raise RuntimeError("AWS credentials not found. Please configure your AWS credentials.")
    except Exception as e:
        raise RuntimeError(f"Error reading S3 file: {e}")
      
if env == 'dev' :
    raw_bkt = 'ddsl-raw-developer'
else :
    raw_bkt = 'ddsl-raw-dev1'
    
# print ('raw_bkt = ', raw_bkt)
bkt_params = read_s3_json(raw_bkt, 'job_config/bucket_config.json')


# Define your bucket names and prefixes
input_bucket_name = bkt_params[env]['RAW_DATA_BKT']
input_prefix = 'batch_landing/'
output_bucket_name = bkt_params[env]['SPLITTED_DATA_BKT']
output_prefix = 'batch_splitted'

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
            write_segregated_files(records, footer_info)

            # Upload segregated files to S3
            upload_files_to_s3(output_bucket_name, output_prefix, records.keys())
else:
    logging.info("No files to process.")









# import boto3
# import sys
# import botocore
# import logging
# from awsglue.utils import getResolvedOptions
# from botocore.exceptions import NoCredentialsError
# import json

# # Configure logging
# logging.basicConfig(level=logging.INFO)

# def list_files_in_s3_bucket(bucket_name, prefix):
#     s3 = boto3.client('s3')
#     file_keys = []
#     try:
#         paginator = s3.get_paginator('list_objects_v2')
#         for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
#             if 'Contents' in page:
#                 file_keys.extend(obj['Key'] for obj in page['Contents'] if obj['Key'] != prefix)
#         if not file_keys:
#             logging.info(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
#     except botocore.exceptions.ClientError as e:
#         logging.error(f"Error listing files in bucket '{bucket_name}' with prefix '{prefix}': {e}")
#     return file_keys

# def read_file_from_s3(bucket_name, file_key):
#     s3 = boto3.client('s3')
#     try:
#         logging.info(f"Attempting to read file '{file_key}' from bucket '{bucket_name}'...")
#         response = s3.get_object(Bucket=bucket_name, Key=file_key)
#         body = response['Body']
#         content = body.read().decode('utf-8')
#         logging.info(f"Successfully read content from '{file_key}'.")
#         return content.splitlines()
#     except botocore.exceptions.ClientError as e:
#         error_code = e.response['Error']['Code']
#         if error_code == 'NoSuchKey':
#             logging.error(f"Error: The file with key '{file_key}' does not exist in bucket '{bucket_name}'.")
#         else:
#             logging.error(f"Unexpected error: {e}")
#         return None

# def segregate_records(lines):
#     records = {}
#     footer_info = {}
#     for line in lines:
#         fields = line.strip().split('|')
#         if len(fields) < 2:
#             continue
#         record_type = fields[0]
#         if record_type == 'T':  # Assuming footer record type is 'T'
#             footer_info = parse_footer(fields)
#         elif record_type not in ('H', 'T'):
#             records.setdefault(record_type, []).append(line)
#     return records, footer_info

# def parse_footer(fields):
#     footer_info = {}
#     for field in fields[1:]:
#         key, value = field.split('~')
#         footer_info[key] = int(value)
#     return footer_info

# def write_segregated_files(records, footer_info):
#     for record_type, lines in records.items():
#         total_records = footer_info.get(record_type, 0)
#         file_name = f"rec_type_{record_type}.txt"  # Updated filename format
#         with open(file_name, 'w') as file:
#             for line in lines:
#                 file.write(line.strip() + f"|{total_records}\n")

# def upload_files_to_s3(bucket_name, prefix, record_types):
#     s3 = boto3.client('s3')
#     for record_type in record_types:
#         file_name = f"rec_type_{record_type}.txt"  # Updated filename format
#         file_key = f"{prefix}/{file_name}"
#         try:
#             with open(file_name, 'rb') as file:
#                 logging.info(f"Uploading file '{file_key}' to bucket '{bucket_name}'...")
#                 s3.upload_fileobj(file, bucket_name, file_key)
#                 logging.info(f"Successfully uploaded file '{file_key}' to bucket '{bucket_name}'.")
#         except Exception as e:
#             logging.error(f"Failed to upload file '{file_key}' to bucket '{bucket_name}': {e}")

# args = getResolvedOptions(sys.argv, ['env'])
# env = args['env']

# # Function to read JSON file from S3
# def read_s3_json(bucket, key):
#     s3 = boto3.client('s3')
#     try:
#         obj = s3.get_object(Bucket=bucket, Key=key)
#         data = obj['Body'].read().decode('utf-8')
#         return json.loads(data)
#     except NoCredentialsError:
#         raise RuntimeError("AWS credentials not found. Please configure your AWS credentials.")
#     except Exception as e:
#         raise RuntimeError(f"Error reading S3 file: {e}")

# if env == 'dev':
#     raw_bkt = 'ddsl-raw-developer'
# else:
#     raw_b



























# # import boto3
# # import sys
# # import botocore
# # import logging
# # from awsglue.utils import getResolvedOptions
# # from botocore.exceptions import NoCredentialsError
# # import json
# # import boto3
# # from urllib.parse import urlparse


# # # Configure logging
# # logging.basicConfig(level=logging.INFO)

# # def list_files_in_s3_bucket(bucket_name, prefix):
# #     s3 = boto3.client('s3')
# #     file_keys = []
# #     try:
# #         paginator = s3.get_paginator('list_objects_v2')
# #         for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
# #             if 'Contents' in page:
# #                 file_keys.extend(obj['Key'] for obj in page['Contents'] if obj['Key'] != prefix)
# #         if not file_keys:
# #             logging.info(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
# #     except botocore.exceptions.ClientError as e:
# #         logging.error(f"Error listing files in bucket '{bucket_name}' with prefix '{prefix}': {e}")
# #     return file_keys

# # def read_file_from_s3(bucket_name, file_key):
# #     s3 = boto3.client('s3')
# #     try:
# #         logging.info(f"Attempting to read file '{file_key}' from bucket '{bucket_name}'...")
# #         response = s3.get_object(Bucket=bucket_name, Key=file_key)
# #         body = response['Body']
# #         content = body.read().decode('utf-8')
# #         logging.info(f"Successfully read content from '{file_key}'.")
# #         return content.splitlines()
# #     except botocore.exceptions.ClientError as e:
# #         error_code = e.response['Error']['Code']
# #         if error_code == 'NoSuchKey':
# #             logging.error(f"Error: The file with key '{file_key}' does not exist in bucket '{bucket_name}'.")
# #         else:
# #             logging.error(f"Unexpected error: {e}")
# #         return None

# # def segregate_records(lines):
# #     records = {}
# #     footer_info = {}
# #     for line in lines:
# #         fields = line.strip().split('|')
# #         if len(fields) < 2:
# #             continue
# #         record_type = fields[0]
# #         if record_type == 'T':  # Assuming footer record type is 'T'
# #             footer_info = parse_footer(fields)
# #         elif record_type not in ('H', 'T'):
# #             records.setdefault(record_type, []).append(line)
# #     return records, footer_info

# # def parse_footer(fields):
# #     footer_info = {}
# #     for field in fields[1:]:
# #         key, value = field.split('~')
# #         footer_info[key] = int(value)
# #     return footer_info

# # def write_segregated_files(records, footer_info):
# #     for record_type, lines in records.items():
# #         total_records = footer_info.get(record_type, 0)
# #         file_name = f"rec_type_{record_type}.txt"  # Updated filename format
# #         with open(file_name, 'w') as file:
# #             for line in lines:
# #                 file.write(line.strip() + f"|{total_records}\n")

# # def upload_files_to_s3(bucket_name, prefix, record_types):
# #     s3 = boto3.client('s3')
# #     for record_type in record_types:
# #         file_name = f"rec_type_{record_type}.txt"  # Updated filename format
# #         file_key = f"{prefix}/{file_name}"
# #         try:
# #             with open(file_name, 'rb') as file:
# #                 logging.info(f"Uploading file '{file_key}' to bucket '{bucket_name}'...")
# #                 s3.upload_fileobj(file, bucket_name, file_key)
# #                 logging.info(f"Successfully uploaded file '{file_key}' to bucket '{bucket_name}'.")
# #         except Exception as e:
# #             logging.error(f"Failed to upload file '{file_key}' to bucket '{bucket_name}': {e}")

# # args = getResolvedOptions(sys.argv,['env'])
# # env = args['env']

# # # Function to read JSON file from S3
# # def read_s3_json(bucket, key):
# #     s3 = boto3.client('s3')
# #     try:
# #         obj = s3.get_object(Bucket=bucket, Key=key)
# #         data = obj['Body'].read().decode('utf-8')
# #         return json.loads(data)
# #     except NoCredentialsError:
# #         raise RuntimeError("AWS credentials not found. Please configure your AWS credentials.")
# #     except Exception as e:
# #         raise RuntimeError(f"Error reading S3 file: {e}")
      
# # if env == 'dev' :
# #     raw_bkt = 'ddsl-raw-developer'
# # else :
# #     raw_bkt = 'ddsl-raw-dev1'
    
# # # print ('raw_bkt = ', raw_bkt)
# # bkt_params = read_s3_json(raw_bkt, 'job_config/bucket_config.json')


# # # Define your bucket names and prefixes
# # input_bucket_name = bkt_params[env]['RAW_DATA_BKT']
# # input_prefix = 'batch_landing/'
# # output_bucket_name = bkt_params[env]['SPLITTED_DATA_BKT']
# # output_prefix = 'batch_splitted'

# # # List all files in the input bucket with the specified prefix
# # file_keys = list_files_in_s3_bucket(input_bucket_name, input_prefix)

# # if file_keys:
# #     for file_key in file_keys:
# #         # Read the file from S3
# #         lines = read_file_from_s3(input_bucket_name, file_key)
# #         if lines:
# #             # Segregate records
# #             records, footer_info = segregate_records(lines)

# #             # Write segregated files locally
# #             write_segregated_files(records, footer_info)

# #             # Upload segregated files to S3
# #             upload_files_to_s3(output_bucket_name, output_prefix, records.keys())
# # else:
# #     logging.info("No files to process.")









# # import sys
# # from awsglue.transforms import *
# # from awsglue.utils import getResolvedOptions
# # from pyspark.context import SparkContext
# # from awsglue.context import GlueContext
# # from pyspark.sql.functions import col, lit
# # import json
# # import boto3
# # from botocore.exceptions import NoCredentialsError, ClientError

# # # Initialize Glue context and Spark session
# # glueContext = GlueContext(SparkContext.getOrCreate())
# # spark = glueContext.spark_session

# # args = getResolvedOptions(sys.argv, ['env'])
# # print('args =========== ', args.keys())
# # env = args['env']

# # # Function to read JSON file from S3
# # def read_s3_json(bucket, key):
# #     s3 = boto3.client('s3')
# #     try:
# #         obj = s3.get_object(Bucket=bucket, Key=key)
# #         data = obj['Body'].read().decode('utf-8')
# #         return json.loads(data)
# #     except NoCredentialsError:
# #         raise RuntimeError("AWS credentials not found. Please configure your AWS credentials.")
# #     except ClientError as e:
# #         raise RuntimeError(f"ClientError: {e}")
# #     except Exception as e:
# #         raise RuntimeError(f"Error reading S3 file: {e}")

# # # Define buckets and paths based on environment
# # if env == 'dev':
# #     try:
# #         # Load bucket parameters (optional) or set the S3 paths directly
# #         raw_data_bucket = "ddsl-raw-developer"
# #         processed_data_bucket = "ddsl-raw-extended-developer" #updated bucket name from dq
# #         print(f"Buckets - Raw: {raw_data_bucket}, Processed: {processed_data_bucket}")
        
# #         # Path to the input file in the raw S3 bucket
# #         input_file_path = "s3://ddsl-raw-developer/batch_landing/AccountExtract_6150_20240902.txt"
        
# #         # Path to save processed files (segregated by rec_type)
# #         failed_records_base_path = f"s3://{processed_data_bucket}/batch_splitted/bad/" #updated folder name
# #         passed_records_base_path = f"s3://{processed_data_bucket}/batch_splitted/good/"
        
# #         # Read the input file from S3 directly into a Spark DataFrame
# #         print("Reading input file from S3...")
# #         spark_df = spark.read.csv(input_file_path, sep='|', header=False, inferSchema=True)
# #         print("Input DataFrame schema:")
# #         spark_df.printSchema()
        
# #         # Assuming `rec_type` is in the first column (adjust column indexing as needed)
# #         rec_type_col = "_c0"  # Update with actual column name or index
        
# #         # Show a sample of the data
# #         print("Sample data:")
# #         spark_df.show(5)
        
# #         # Filter records based on `rec_type` and store in different S3 paths
# #         rec_types = spark_df.select(rec_type_col).distinct().rdd.flatMap(lambda x: x).collect()
        
# #         for rec_type in rec_types:
# #             print(f"Processing records for rec_type = {rec_type}...")
            
# #             # Filter records for the current `rec_type`
# #             rec_type_df = spark_df.filter(spark_df[rec_type_col] == rec_type)
            
# #             # Define paths for passed and failed records for the current `rec_type`
# #             failed_records_path = f"{failed_records_base_path}{rec_type}/"
# #             passed_records_path = f"{passed_records_base_path}{rec_type}/"
            
# #             # Here, you can apply any additional transformation or filtering based on your requirements
# #             # For example, let's assume records with nulls are considered 'failed'
# #             failed_df = rec_type_df.filter(col("_c1").isNull())  # Example: Adjust column for failure condition
# #             passed_df = rec_type_df.filter(col("_c1").isNotNull())  # Adjust as per rules
            
# #             # Show filtered records
# #             print(f"Failed records for rec_type = {rec_type}:")
# #             failed_df.show()
# #             print(f"Passed records for rec_type = {rec_type}:")
# #             passed_df.show()
            
# #             # Write failed and passed records back to S3
# #             print(f"Writing failed records to {failed_records_path}...")
# #             failed_df.write.csv(failed_records_path, mode='overwrite')
            
# #             print(f"Writing passed records to {passed_records_path}...")
# #             passed_df.write.csv(passed_records_path, mode='overwrite')

# #     except RuntimeError as e:
# #         print(f"RuntimeError occurred: {e}")
# #         sys.exit(1)

# #     except Exception as e:
# #         print(f"An unexpected error occurred: {e}")
# #         sys.exit(1)
# # else:
# #     print(f"Invalid environment '{env}'.")
# #     sys.exit(1)




