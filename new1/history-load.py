import logging
import json
import boto3
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from botocore.exceptions import NoCredentialsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_config_from_s3(bucket_name, file_key):
    s3_client = boto3.client('s3')
    try:
        logger.info(f"Reading configuration from s3://{bucket_name}/{file_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        config_content = response['Body'].read().decode('utf-8')
        config = json.loads(config_content)
        logger.info("Configuration loaded successfully from S3.")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading config from S3: {e}", exc_info=True)
        sys.exit(1)

def get_hudi_options(table_name, database_name, primary_keys, precombine_field):
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': ','.join(primary_keys),
        'hoodie.datasource.write.precombine.field': precombine_field,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.partitionpath.field': 'dt',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': database_name,
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
        'hoodie.cleaner.commits.retained': 0,
        'hoodie.cleaner.commits.retained.max': 7,
        'hoodie.cleaner.parallelism': 200,
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.datasource.hive_sync.use_jdbc': 'false',
        'hoodie.datasource.write.schema.evolution': 'true'
    }

def clean_column_names(df):
    new_columns = [col_name.strip().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").lower() for col_name in df.columns]
    for old_name, new_name in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_name, new_name)
    logger.info(f"Renamed columns: {dict(zip(df.columns, new_columns))}")
    return df

def write_to_hudi(data_df, hudi_path, hudi_options):
    try:
        data_df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)
        logger.info(f"Successfully updated the Hudi table at {hudi_path}")
    except Exception as e:
        logger.error(f"Error during Hudi table update at {hudi_path}: {e}")
        sys.exit(1)

def process_rec_type(spark, rec_type, glueContext, rec_type_config, src_data_bkt):
    logger.info(f"Processing record type: {rec_type}")
    primary_keys = rec_type_config.get("primary_keys", [])
    table_name = f"{rec_type}"
    database_name = rec_type_config.get("database_name", "")
    hudi_table_path = f"s3://{rec_type_config.get('hudi_table_path', '')}"

    source_path = f's3://{src_data_bkt}/{rec_type_config.get("source_path", "")}'
    
    if check_s3_directory_exists(src_data_bkt, rec_type_config.get("source_path", "")):
        org_parquet_df = spark.read.format("parquet").load(source_path)
        org_parquet_df = clean_column_names(org_parquet_df)

        hudi_options = get_hudi_options(table_name, database_name, primary_keys, rec_type_config.get("Batch_date", ""))
        write_to_hudi(org_parquet_df, hudi_table_path, hudi_options)
    else:
        logger.error(f"S3 directory does not exist: {src_data_bkt}/{rec_type_config.get('source_path', '')}")
        raise RuntimeError(f"S3 directory does not exist: {src_data_bkt}/{rec_type_config.get('source_path', '')}")

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_data_bkt', 'rec_type', 'config_file'])
    job_name = args['JOB_NAME']
    src_data_bkt = args['src_data_bkt']
    rec_type = args['rec_type']
    config_file = args['config_file']

    logger.info(f"Job name: {job_name}")
    logger.info(f"Source data bucket: {src_data_bkt}")
    logger.info(f"Record type: {rec_type}")
    logger.info(f"Configuration file: {config_file}")

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, args)

    config = read_config_from_s3(src_data_bkt, config_file)
    rec_type_config = config.get(rec_type, {})

    process_rec_type(spark, rec_type, glueContext, rec_type_config, src_data_bkt)

    job.commit()

if __name__ == "__main__":
    main()
