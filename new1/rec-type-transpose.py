import logging
import json
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, current_date, when
import sys

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
        'hoodie.datasource.meta_sync.condition.sync': 'true',
        'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
        'hoodie.cleaner.commits.retained': 0,
        'hoodie.cleaner.commits.retained.max': 7,
        'hoodie.cleaner.parallelism': 200,
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.datasource.hive_sync.use_jdbc': 'false',
        'hoodie.datasource.write.schema.evolution': 'true'
    }

def hudi_table_exists(spark, path: str) -> bool:
    try:
        hudi_df = spark.read.format("hudi").load(path)
        exists = hudi_df.count() > 0
        logger.info(f"Hudi table existence check at path {path}: {'Exists' if exists else 'Does not exist'}")
        return exists
    except Exception as e:
        logger.error(f"Error checking Hudi table existence at {path}: {e}")
        return False

def clean_column_names(df):
    try:
        new_columns = [col_name.strip().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").lower() for col_name in df.columns]
        for old_name, new_name in zip(df.columns, new_columns):
            df = df.withColumnRenamed(old_name, new_name)
        logger.info(f"Renamed columns: {dict(zip(df.columns, new_columns))}")
        return df
    except Exception as e:
        logger.error(f"Error cleaning column names: {e}")
        sys.exit(1)

def create_insert_df(df, batch_date, delete_indicator_column):
    try:
        insert_df = df.withColumn("record_start_date", to_date(col(batch_date), "yyyy-MM-dd")) \
                      .withColumn("record_end_date", to_date(lit("9999-12-31"), "yyyy-MM-dd")) \
                      .withColumn("current_record_flag", lit(1)) \
                      .withColumn("dt", current_date())
        if delete_indicator_column:
            insert_df = insert_df.withColumn(
                "delete_flag",
                when(col(delete_indicator_column) == lit('D'), lit(1))
                .otherwise(lit(0))
            )
        else:
            insert_df = insert_df.withColumn("delete_flag", lit(0))
        
        logger.info("Insert DataFrame created successfully.")
        return insert_df
    except Exception as e:
        logger.error(f"Error creating insert DataFrame: {e}", exc_info=True)
        sys.exit(1)

def write_to_hudi(data_df, hudi_path, hudi_options):
    try:
        data_df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)
        logger.info(f"Successfully updated the Hudi table at {hudi_path}")
    except Exception as e:
        logger.error(f"Error during Hudi table update at {hudi_path}: {e}")
        sys.exit(1)

def process_rec_type(spark, rec_type, glueContext, rec_type_config, parquet_file_path, hudi_table_path, src_data_bkt):
    logger.info(f"Processing record type: {rec_type}")
    primary_keys = rec_type_config.get("primary_keys", [])
    joining_key = rec_type_config.get("joining_key", [])
    table_name = f"{rec_type}"
    database_name = rec_type_config.get("database_name", "")
    precombine_field = rec_type_config.get("Batch_date", "")
    source_path = f's3://{src_data_bkt}/{rec_type_config.get("source_path", "")}' 
    batch_date = rec_type_config.get("Batch_date", "")
    delete_indicator_column = rec_type_config.get("Delete_indicator_column", "")
    hudi_options = get_hudi_options(table_name, database_name, primary_keys, precombine_field)

    try:
        if check_s3_directory_exists(src_data_bkt, rec_type_config.get("source_path", "")):
            org_parquet_df = spark.read.format("parquet").load(source_path)
            org_parquet_df = clean_column_names(org_parquet_df)
            src_insert_df = create_insert_df(org_parquet_df, batch_date, delete_indicator_column)
            src_insert_df = src_insert_df.select([col(c).alias(c.lower()) for c in src_insert_df.columns])
            if hudi_table_exists(spark, hudi_table_path):
                latest_hudi_df = spark.read.format("hudi").load(hudi_table_path).filter((col('current_record_flag') == 1) & (col('delete_flag') == 0)).select(*src_insert_df.columns)
                write_to_hudi(latest_hudi_df, hudi_table_path, hudi_options)
                write_to_hudi(src_insert_df, hudi_table_path, hudi_options)
            else:
                write_to_hudi(src_insert_df, hudi_table_path, hudi_options)
        else:
            logger.error(f"S3 directory does not exist: {src_data_bkt}/{rec_type_config.get('source_path', '')}")
            raise RuntimeError(f"S3 directory does not exist: {src_data_bkt}/{rec_type_config.get('source_path', '')}")
    except Exception as e:
        logger.error(f"Error processing record type {rec_type}: {e}")
        sys.exit(1)

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
    parquet_file_path = f"s3://{src_data_bkt}/{rec_type_config.get('source_path', '')}"
    hudi_table_path = f"s3://{rec_type_config.get('hudi_table_path', '')}"

    process_rec_type(spark, rec_type, glueContext, rec_type_config, parquet_file_path, hudi_table_path, src_data_bkt)

    job.commit()

if __name__ == "__main__":
    main()
