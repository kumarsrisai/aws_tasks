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




