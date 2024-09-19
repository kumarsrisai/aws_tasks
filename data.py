segregate.py

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Initialize Glue context and Spark session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['env'])
print('args =========== ', args.keys())
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
    except ClientError as e:
        raise RuntimeError(f"ClientError: {e}")
    except Exception as e:
        raise RuntimeError(f"Error reading S3 file: {e}")

# Define buckets and paths based on environment
if env == 'dev':
    try:
        # Load bucket parameters (optional) or set the S3 paths directly
        raw_data_bucket = "ddsl-raw-developer"
        processed_data_bucket = "ddsl-raw-extended-developer" #updated bucket name from dq
        print(f"Buckets - Raw: {raw_data_bucket}, Processed: {processed_data_bucket}")
        
        # Path to the input file in the raw S3 bucket
        input_file_path = "s3://ddsl-raw-developer/batch_landing/AccountExtract_6150_20240902.txt"
        
        # Path to save processed files (segregated by rec_type)
        failed_records_base_path = f"s3://{processed_data_bucket}/batch_splitted/bad/" #updated folder name
        passed_records_base_path = f"s3://{processed_data_bucket}/batch_splitted/good/"
        
        # Read the input file from S3 directly into a Spark DataFrame
        print("Reading input file from S3...")
        spark_df = spark.read.csv(input_file_path, sep='|', header=False, inferSchema=True)
        print("Input DataFrame schema:")
        spark_df.printSchema()
        
        # Assuming `rec_type` is in the first column (adjust column indexing as needed)
        rec_type_col = "_c0"  # Update with actual column name or index
        
        # Show a sample of the data
        print("Sample data:")
        spark_df.show(5)
        
        # Filter records based on `rec_type` and store in different S3 paths
        rec_types = spark_df.select(rec_type_col).distinct().rdd.flatMap(lambda x: x).collect()
        
        for rec_type in rec_types:
            print(f"Processing records for rec_type = {rec_type}...")
            
            # Filter records for the current `rec_type`
            rec_type_df = spark_df.filter(spark_df[rec_type_col] == rec_type)
            
            # Define paths for passed and failed records for the current `rec_type`
            failed_records_path = f"{failed_records_base_path}{rec_type}/"
            passed_records_path = f"{passed_records_base_path}{rec_type}/"
            
            # Here, you can apply any additional transformation or filtering based on your requirements
            # For example, let's assume records with nulls are considered 'failed'
            failed_df = rec_type_df.filter(col("_c1").isNull())  # Example: Adjust column for failure condition
            passed_df = rec_type_df.filter(col("_c1").isNotNull())  # Adjust as per rules
            
            # Show filtered records
            print(f"Failed records for rec_type = {rec_type}:")
            failed_df.show()
            print(f"Passed records for rec_type = {rec_type}:")
            passed_df.show()
            
            # Write failed and passed records back to S3
            print(f"Writing failed records to {failed_records_path}...")
            failed_df.write.csv(failed_records_path, mode='overwrite')
            
            print(f"Writing passed records to {passed_records_path}...")
            passed_df.write.csv(passed_records_path, mode='overwrite')

    except RuntimeError as e:
        print(f"RuntimeError occurred: {e}")
        sys.exit(1)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
else:
    print(f"Invalid environment '{env}'.")
    sys.exit(1)






DQ1.py

import boto3
import os
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize S3 client
s3 = boto3.client('s3')

# Bucket names
input_bucket = 'ddsl-raw-extended-developer'
output_bucket = 'ddsl-dq-developer'

# Function to move file to another folder in S3
def move_file(source_key, status):
    destination_folder = f'batch_dq/{status}/'
    s3.copy_object(
        Bucket=output_bucket,
        CopySource={'Bucket': input_bucket, 'Key': source_key},
        Key=f'{destination_folder}{os.path.basename(source_key)}'
    )
    # Uncomment the next line if you want to delete the source file
    # s3.delete_object(Bucket=input_bucket, Key=source_key)
    print(f"File {source_key} moved to {destination_folder}")

# List files in input bucket
response = s3.list_objects_v2(Bucket=input_bucket, Prefix='batch_splitted/')

# Check if there are any files to process
if 'Contents' not in response:
    print("No files found to process.")
else:
    # Iterate through each file
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        file_name = os.path.basename(file_key)

        # Skip directories
        if file_key.endswith('/'):
            continue

        print(f"Processing file: {file_name}")

        # Read file from S3 into a Spark DataFrame
        s3_path = f's3://{input_bucket}/{file_key}'
        try:
            df = spark.read.option('delimiter', '|').csv(s3_path)

            # Ensure the DataFrame has data
            if df.count() == 0:
                print(f"No data found in file {file_name}")
                move_file(file_key, 'bad')
                continue

            # Get the last row for reference row count
            last_row = df.orderBy(df['_c0'].desc()).limit(1).collect()

            if last_row:
                # Extract value from the last row and handle None values
                last_row_values = last_row[0]
                if last_row_values and len(last_row_values) > 0:
                    try:
                        reference_row_count = int(last_row_values[-1])  # Convert to int
                    except (ValueError, TypeError) as e:
                        print(f"Error converting reference row count to int for file {file_name}: {e}")
                        move_file(file_key, 'bad')
                        continue
                else:
                    print(f"Reference row count not found in file {file_name}")
                    move_file(file_key, 'bad')
                    continue
            else:
                print(f"No last row found in file {file_name}")
                move_file(file_key, 'bad')
                continue

            # Count actual rows in the DataFrame
            actual_row_count = df.count()

            # Compare row counts
            print(f"File {file_name} - Actual: {actual_row_count}, Reference: {reference_row_count}")
            if actual_row_count != reference_row_count:
                print(f"Row count mismatch for file {file_name}: Actual: {actual_row_count}, Reference: {reference_row_count}")
                move_file(file_key, 'bad')  # Move to 'bad' folder if counts don't match
            else:
                print(f"Row count match for file {file_name}: {actual_row_count}")
                move_file(file_key, 'good')  # Move to 'good' folder if counts match

        except Exception as e:
            print(f"Error processing file {file_name}: {e}")
            move_file(file_key, 'bad')

# import boto3
# import os
# from awsglue.context import GlueContext
# from pyspark.context import SparkContext
# from pyspark.sql import SparkSession

# # Initialize Spark and Glue contexts
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# # Initialize S3 client
# s3 = boto3.client('s3')

# # Bucket names
# input_bucket = 'ddsl-raw-extended-developer'
# output_bucket = 'ddsl-dq-developer'

# # Function to move file to another folder in S3
# def move_file(source_key, status):
#     destination_folder = f'batch_dq/{status}'
#     s3.copy_object(
#         Bucket=output_bucket,
#         CopySource={'Bucket': input_bucket, 'Key': source_key},
#         Key=f'{destination_folder}/{os.path.basename(source_key)}'
#     )
#     s3.delete_object(Bucket=input_bucket, Key=source_key)

# # List files in input bucket
# response = s3.list_objects_v2(Bucket=input_bucket, Prefix='batch_splitted/')

# # Iterate through each file
# for obj in response.get('Contents', []):
#     file_key = obj['Key']
#     file_name = os.path.basename(file_key)

#     # Read file from S3 into a Spark DataFrame
#     s3_path = f's3://{input_bucket}/{file_key}'
#     df = spark.read.option('delimiter', '|').csv(s3_path)

#     # Ensure the DataFrame has data
#     if df.count() == 0:
#         print(f"No data found in file {file_name}")
#         move_file(file_key, 'bad')
#         continue

#     # Get the last row for reference row count
#     last_row = df.orderBy(df['_c0'].desc()).limit(1).collect()
#     if last_row:
#         # Extract value from the last row and handle None values
#         last_row_values = last_row[0]
#         if last_row_values and len(last_row_values) > 0:
#             try:
#                 reference_row_count = int(last_row_values[-1])  # Convert to int
#             except (ValueError, TypeError) as e:
#                 print(f"Error converting reference row count to int for file {file_name}: {e}")
#                 move_file(file_key, 'bad')
#                 continue
#         else:
#             print(f"Reference row count not found in file {file_name}")
#             move_file(file_key, 'bad')
#             continue
#     else:
#         print(f"No last row found in file {file_name}")
#         move_file(file_key, 'bad')
#         continue

#     # Count actual rows in the DataFrame
#     actual_row_count = df.count()

#     # Compare row counts
#     if actual_row_count != reference_row_count:
#         print(f"Row count mismatch for file {file_name}: Actual: {actual_row_count}, Reference: {reference_row_count}")
#         move_file(file_key, 'bad')  # Move to 'bad' folder inside batch_dq_checksum if counts don't match
#     else:
#         print(f"Row count match for file {file_name}: {actual_row_count}")
#         move_file(file_key, 'good')  # Move to 'good' folder inside batch_dq_checksum if counts match


DQ2.py





transpose_config.json



// {
//     "rec_type_9005": {
//         "select_cols": ["AR_ID_ITEM", "BALTT_CODE", "AABDP_AMT","FULL_DATE"],
//         "group_by_cols": ["AR_ID_ITEM","FULL_DATE"],
//         "pivot_cols": ["BALTT_CODE"],
//         "agg_cols": ["AABDP_AMT"]
//     },
//     "rec_type_9019": {
//         "select_cols": ["AR_ID_ITEM", "BALTT_CODE", "AABDP_AMT","FULL_DATE"],
//         "group_by_cols": ["AR_ID_ITEM","FULL_DATE"],
//         "pivot_cols": ["BALTT_CODE"],
//         "agg_cols": ["AABDP_AMT"]
//     }

// }
