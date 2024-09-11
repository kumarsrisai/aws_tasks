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
    s3.delete_object(Bucket=input_bucket, Key=source_key)

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
            if actual_row_count != reference_row_count:
                print(f"Row count mismatch for file {file_name}: Actual: {actual_row_count}, Reference: {reference_row_count}")
                move_file(file_key, 'bad')  # Move to 'bad' folder inside batch_dq if counts don't match
            else:
                print(f"Row count match for file {file_name}: {actual_row_count}")
                move_file(file_key, 'good')  # Move to 'good' folder inside batch_dq if counts match

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