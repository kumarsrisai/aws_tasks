import sys
import json
from urllib.parse import urlparse
import boto3
from botocore.exceptions import NoCredentialsError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import first


# Retrieve the parameters passed to the Glue job
args = getResolvedOptions(sys.argv, ['rec_type', 'env'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['rec_type', 'env'], args)
# job.init(args)

rec_type = args['rec_type']
env = args['env']

# Function to check if a file exists in S3
def check_s3_directory_exists(bucket: str, prefix: str) -> bool:
    s3_client = boto3.client('s3')
    """
    Check if the S3 directory (prefix) exists.

    :param bucket: The S3 bucket name.
    :param prefix: The S3 prefix (directory path).
    :return: True if the directory exists, False otherwise.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    return 'Contents' in response or 'CommonPrefixes' in response


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
    
print ('raw_bkt = ', raw_bkt)
bkt_params = read_s3_json(raw_bkt, 'job_config/bucket_config.json')


# Load parameters from S3
params = read_s3_json(bkt_params[env]['RAW_DATA_BKT'], 'job_config/transpose.json')
rec_type_params = params[rec_type]

# rec_type = 'rec_type_9005'
select_cols = rec_type_params['select_cols']
group_by_cols = rec_type_params['group_by_cols']
pivot_cols = rec_type_params['pivot_cols']
agg_cols = rec_type_params['agg_cols']

input_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq/good/{rec_type}/"
output_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/transpose_output/{rec_type}/"

# Extract bucket and key from input_file_path
parsed_url = urlparse(input_path)
input_bucket = parsed_url.netloc
input_key = parsed_url.path.lstrip('/')
print ('input_bucket = ', input_bucket)
print ('input_key = ', input_key)

if check_s3_directory_exists(input_bucket, input_key):
    rec_type_df =  spark.read.parquet(input_path)
    # Display the DataFrame to understand its structure
    rec_type_df.show(truncate=False)
    print ('rec_type_df = ', rec_type_df.count())
    
    rec_type_df_selected=rec_type_df.select(select_cols)
    rec_type_df_selected.show()
    
    # Pivot the DataFrame to transform BALTT_CODE values into columns
    rec_type_pivot_df = rec_type_df_selected.groupBy(group_by_cols).pivot(*pivot_cols).agg(first(*agg_cols))
    rec_type_pivot_df.write.mode('overwrite').parquet(output_path)
else :
    print(f'File {input_key} does not exist in bucket {input_bucket}.')
# job.commit()