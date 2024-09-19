import boto3
import os
import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import botocore
import logging
from awsglue.utils import getResolvedOptions
from botocore.exceptions import NoCredentialsError
import json
from urllib.parse import urlparse

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize S3 client
s3 = boto3.client('s3')

# Get environment argument from AWS Glue job
args = getResolvedOptions(sys.argv, ['env'])
env = args['env']
pass_status = 'pass'
fail_status = 'fail'

# Function to read JSON file from S3
def read_s3_json(bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return json.loads(data)
    except NoCredentialsError:
        raise RuntimeError("AWS credentials not found. Please configure your AWS credentials.")
    except Exception as e:
        raise RuntimeError(f"Error reading S3 file: {e}")

# Set bucket based on environment
if env == 'dev':
    raw_bkt = 'ddsl-raw-developer'
else:
    raw_bkt = 'ddsl-raw-dev1'

# Read bucket configuration from S3
bkt_params = read_s3_json(raw_bkt, 'job_config/bucket_config.json')

# Set input and output bucket names
input_bucket = bkt_params[env]['SPLITTED_DATA_BKT']
output_bucket = bkt_params[env]['DQ_DATA_BKT']

# Function to move file to another folder in S3
def move_file(source_key, status):
    destination_folder = f'batch_dq_checksum/accounting/{status}'
    s3.copy_object(
        Bucket=output_bucket,
        CopySource={'Bucket': input_bucket, 'Key': source_key},
        Key=f'{destination_folder}/{os.path.basename(source_key)}'
    )
    s3.delete_object(Bucket=input_bucket, Key=source_key)

# Define schema for the CSV file
schema = StructType([
    StructField("_c0", IntegerType(), True),   # Replace with actual column names/types
    StructField("_c1", StringType(), True),
    StructField("_c2", StringType(), True),
    StructField("_c3", IntegerType(), True)    # Assuming the last column is the reference row count
])

# List files in the input bucket
response = s3.list_objects_v2(Bucket=input_bucket, Prefix='batch_splitted/')

# Iterate through each file
for obj in response.get('Contents', []):
    file_key = obj['Key']
    file_name = os.path.basename(file_key)

    # Read file from S3 into a Spark DataFrame with defined schema
    s3_path = f's3://{input_bucket}/{file_key}'
    print('input_bucket =', input_bucket)
    print('file_key =', file_key)
    print('s3_path =', s3_path)

    df = spark.read.option('delimiter', '|').schema(schema).csv(s3_path)

    # Ensure the DataFrame has data
    if df.count() == 0:
        print(f"No data found in file {file_name}")
        move_file(file_key, fail_status)
        continue

    # Get the last row for reference row count
    last_row = df.orderBy(df['_c0'].desc()).limit(1).collect()
    if last_row:
        last_row_values = last_row[0]
        if last_row_values and len(last_row_values) > 0:
            try:
                reference_row_count = int(last_row_values[-1])  # Convert to int
            except (ValueError, TypeError) as e:
                print(f"Error converting reference row count to int for file {file_name}: {e}")
                move_file(file_key, fail_status)
                continue
        else:
            print(f"Reference row count not found in file {file_name}")
            move_file(file_key, fail_status)
            continue
    else:
        print(f"No last row found in file {file_name}")
        move_file(file_key, fail_status)
        continue

    # Count actual rows in the DataFrame
    actual_row_count = df.count()

    # Compare row counts
    if actual_row_count != reference_row_count:
        print(f"Row count mismatch for file {file_name}: Actual: {actual_row_count}, Reference: {reference_row_count}")
        move_file(file_key, fail_status)  # Move to 'fail' folder if counts don't match
    else:
        print(f"Row count match for file {file_name}: {actual_row_count}")
        move_file(file_key, pass_status)  # Move to 'pass' folder if counts match
