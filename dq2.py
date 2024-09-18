import sys
import json
import re
import boto3
from botocore.exceptions import NoCredentialsError
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from awsglue.transforms import SelectFromCollection
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, DecimalType, StructField, StructType

# Initialize Spark and Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Retrieve job parameters
args = getResolvedOptions(sys.argv, ['rec_type', 'env'])
rec_type = args['rec_type']
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

# Function to read text file from S3
def read_s3_text(bucket, key):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return data
    except NoCredentialsError:
        raise RuntimeError("AWS credentials not found. Please configure your AWS credentials.")
    except Exception as e:
        raise RuntimeError(f"Error reading S3 file: {e}")

# Function to convert decimal type string to DecimalType
def get_decimal_type(decimal_str):
    match = re.match(r"decimal\((\d+),(\d+)\)", decimal_str)
    if match:
        precision = int(match.group(1))
        scale = int(match.group(2))
        return DecimalType(precision, scale)
    else:
        raise ValueError(f"Unsupported decimal type format: {decimal_str}")

# Function to check if a file exists in S3
def s3_file_exists(bucket_name, file_key):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

# Define environment-specific bucket
raw_bkt = 'ddsl-raw-cg-vl' if env == 'cg_dev' else 'ddsl-raw-dev1'

# Load bucket parameters from S3
bkt_params = read_s3_json(raw_bkt, 'job_config/batch_config/bucket_config.json')

# Retrieve file paths
input_file_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq_checksum/accounting/pass/{rec_type}.txt"
rules_file_path = f"s3://{bkt_params[env]['RAW_DATA_BKT']}/job_config/batch_config/ruleset.json"
failed_records_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq_recordlevel/accounting/fail/{rec_type}/"
passed_records_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq_recordlevel/accounting/pass/{rec_type}/"

# Extract bucket and key from input_file_path
parsed_url = urlparse(input_file_path)
input_bucket = parsed_url.netloc
input_key = parsed_url.path.lstrip('/')
print ('input_bucket = ', input_bucket)
print ('input_key = ', input_key)

# Check if the input file exists
if s3_file_exists(input_bucket, input_key):
    # Read the input file from S3 directly into a Spark DataFrame
    spark_df = spark.read.csv(
        input_file_path,
        sep='|',
        header=False,
        quote='"',
        inferSchema=True
    )

    # Read the column definitions from S3
    column_details = read_s3_json(bkt_params[env]['RAW_DATA_BKT'], 'job_config/batch_config/columns.json')
    
    # Check if rec_type exists in column details
    if rec_type not in column_details:
        raise ValueError(f"Rec_type {rec_type} not found in column details")
    
    # Get the column definitions for the specified rec_type
    rec_type_columns = column_details[rec_type]["columns"]

    # Define a mapping for data types
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "date": StringType(),
        "timestamp": TimestampType()
    }

    # Create a list of StructField for the schema
    fields = []
    for col_def in rec_type_columns:
        col_name = col_def['name']
        col_type = col_def['type']
        if col_type in type_mapping:
            fields.append(StructField(col_name, type_mapping[col_type], True))
        elif col_type.startswith('decimal'):
            fields.append(StructField(col_name, get_decimal_type(col_type), True))
        else:
            raise ValueError(f"Unsupported type: {col_type}")

    # Define the schema
    schema = StructType(fields)

    # Read the input file with the defined schema
    spark_df = spark.read.csv(
        input_file_path,
        sep='|',
        header=False,
        quote='"',
        schema=schema
    )

    # Identify columns that include '_DATE'
    date_columns = [c for c in spark_df.columns if '_DATE' in c]
    
    # Apply conversion to DateType for each identified column
    for date_col in date_columns:
        spark_df = spark_df.withColumn(date_col, to_date(col(date_col), 'yyyyMMdd'))

    # Load and process ruleset from S3
    parsed_url = urlparse(rules_file_path)
    rules_bucket = parsed_url.netloc
    rules_key = parsed_url.path.lstrip('/')
    
    all_ruleset_str = read_s3_text(rules_bucket, rules_key)
    all_ruleset_dict = json.loads(all_ruleset_str)
    rec_type_ruleset = all_ruleset_dict.get(rec_type, '').replace("'", '"')
    ruleset = f"""Rules = {rec_type_ruleset}"""

    # Convert Spark DataFrame to Glue DynamicFrame
    rule_AmazonS3 = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_name")

    # Data quality evaluation
    evaluation_result = EvaluateDataQuality().process_rows(
        frame=rule_AmazonS3,
        ruleset=ruleset,
        publishing_options={
            "dataQualityEvaluationContext": "EvaluateDataQuality",
            "enableDataQualityCloudWatchMetrics": False,
            "enableDataQualityResultsPublishing": False
        },
        additional_options={"observations.scope": "ALL", "performanceTuning.caching": "CACHE_NOTHING"}
    )

    # Extract rule outcomes and filter records
    rule_outcomes = SelectFromCollection.apply(dfc=evaluation_result, key="ruleOutcomes", transformation_ctx="ruleOutcomes")
    rule_outcomes.toDF().show(truncate=False)
    
    rowLevelOutcomes = SelectFromCollection.apply(
        dfc=evaluation_result,
        key="rowLevelOutcomes",
        transformation_ctx="rowLevelOutcomes"
    )

    rowLevelOutcomes_df = rowLevelOutcomes.toDF()  # Convert Glue DynamicFrame to Spark DataFrame
    rowLevelOutcomes_df_passed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Passed")  # Filter only the Passed records
    rowLevelOutcomes_df_failed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Failed")  # Review the Failed records 

    # Convert DataFrames back to DynamicFrames
    dynamic_frame_passed = DynamicFrame.fromDF(rowLevelOutcomes_df_passed, glueContext, "dynamic_frame_passed")
    dynamic_frame_failed = DynamicFrame.fromDF(rowLevelOutcomes_df_failed, glueContext, "dynamic_frame_failed")

    # Write Passed and Failed records to S3
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_passed,
        connection_type="s3",
        connection_options={"path": passed_records_path},
        format="parquet"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_failed,
        connection_type="s3",
        connection_options={"path": failed_records_path},
        format="parquet"
    )

else:
    print(f"File {input_key} does not exist in bucket {input_bucket}.")
    
# Complete the job successfully without processing
job.commit()
