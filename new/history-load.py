import logging
import json
import boto3
from urllib.parse import urlparse
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import sys
from functools import reduce

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to read JSON configuration from S3
def read_config_from_s3(bucket_name, file_key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        config_content = response['Body'].read().decode('utf-8')
        logger.info(f"Configuration content: {config_content}")
        config = json.loads(config_content)
        logger.info("Configuration loaded successfully from S3.")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error reading config from S3: {e}", exc_info=True)
        raise

# Hudi options generator
def get_hudi_options(table_name, database_name, primary_keys, precombine_field):
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': ','.join(primary_keys),
        'hoodie.datasource.write.precombine.field': precombine_field,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.partitionpath.field': "dt",
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': database_name,
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.datasource.meta_sync.condition.sync': 'true',
        'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
        'hoodie.cleaner.commits.retained': 0,
        'hoodie.cleaner.commits.retained.max': 7,
        'hoodie.cleaner.parallelism': 200,
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.datasource.hive_sync.use_jdbc': 'false',
        'hoodie.datasource.write.schema.evolution': 'true'
    }

# Check if Hudi table exists
def hudi_table_exists(spark, path: str) -> bool:
    try:
        hudi_df = spark.read.format("hudi").load(path)
        exists = hudi_df.count() > 0
        logger.info(f"Hudi table existence check at path {path}: {'Exists' if exists else 'Does not exist'}")
        return exists
    except Exception as e:
        logger.error(f"Error checking Hudi table existence at {path}: {e}")
        return False

# Clean up column names
def clean_column_names(df):
    try:
        new_columns = [col_name.strip().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").lower() for col_name in df.columns]
        for old_name, new_name in zip(df.columns, new_columns):
            df = df.withColumnRenamed(old_name, new_name)
        logger.info(f"Renamed columns: {dict(zip(df.columns, new_columns))}")
        return df
    except Exception as e:
        logger.error(f"Error cleaning column names: {e}", exc_info=True)
        raise

# Write to Hudi table
def write_to_hudi(data_df, hudi_path, hudi_options):
    try:
        data_df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)
        logger.info(f"Successfully updated the Hudi table at {hudi_path}")
    except Exception as e:
        logger.error(f"Error during Hudi table update at {hudi_path}: {e}", exc_info=True)
        raise

# Read S3 JSON file
def read_s3_json(bucket, key):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return json.loads(data)
    except Exception as e:
        logger.error(f"Error reading S3 file: {e}", exc_info=True)
        raise

# Check if S3 directory exists
def check_s3_directory_exists(bucket: str, prefix: str) -> bool:
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    return 'Contents' in response or 'CommonPrefixes' in response

# Process record type
def process_rec_type(spark, rec_type, rec_type_config, transpose_history_hudi_table_path, src_data_bkt):
    logger.info(f"Processing record type: {rec_type}")
    primary_keys = rec_type_config.get("primary_keys", [])
    database_name = rec_type_config.get("database_name", "")
    precombine_field = rec_type_config.get("Batch_date", "")
    source_path = f's3://{src_data_bkt}/{rec_type_config.get("source_path", "")}'
    
    history_table_name = "transpose_history_" + rec_type
    history_hudi_options = get_hudi_options(history_table_name, database_name, primary_keys, precombine_field)

    if check_s3_directory_exists(src_data_bkt, rec_type_config.get("source_path", "")):
        org_parquet_df = spark.read.format("parquet").load(source_path)
        org_parquet_df = org_parquet_df.drop("DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip", "DataQualityEvaluationResult")
        cleaned_df = clean_column_names(org_parquet_df)
        cleaned_df = cleaned_df.withColumn("dt", current_date())

        if hudi_table_exists(spark, transpose_history_hudi_table_path):
            logger.info("Appending records to existing Hudi table.")
        else:
            logger.info("Creating a new Hudi table.")

        write_to_hudi(cleaned_df, transpose_history_hudi_table_path, history_hudi_options)
    else:
        logger.error(f"Source path {rec_type_config.get('source_path', '')} does not exist in bucket {src_data_bkt}.")

# Main function
def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ['rec_type', 'env'])
    rec_type = args['rec_type']
    env = args['env']

    raw_bkt = 'ddsl-raw-developer' if env == 'dev' else 'ddsl-raw-dev1'
    
    bkt_params = read_s3_json(raw_bkt, 'job_config/bucket_config.json')
    parquet_file_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq_recordlevel/accounting/pass/{rec_type}/"
    transpose_history_hudi_table_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/ddsl-transpose-history-hudi/{rec_type}/"

    config = read_config_from_s3(bkt_params[env]['RAW_DATA_BKT'], 'job_config/transpose_config.json')

    try:
        logger.info("Starting the ETL job")
        rec_type_param = config[rec_type]
        process_rec_type(spark, rec_type, rec_type_param, transpose_history_hudi_table_path, bkt_params[env]['DQ_DATA_BKT'])
        logger.info("ETL job completed successfully")
    except KeyError as e:
        logger.error(f"Configuration for record type '{rec_type}' not found: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during ETL job: {e}", exc_info=True)
        sys.exit(1)
    finally:
        job.commit()

if __name__ == "__main__":
    main()
