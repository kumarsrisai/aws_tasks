import logging
import json
import boto3
import botocore
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
from awsglue.utils import getResolvedOptions
from botocore.exceptions import NoCredentialsError
import json
import boto3
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_config_from_s3(bucket_name, file_key):
    s3_client = boto3.client('s3')
    try:
        print ('bucket_name -____________ ', bucket_name, file_key)
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        config_content = response['Body'].read().decode('utf-8')
        logger.info(f"Configuration content: {config_content}")  
        config = json.loads(config_content)
        logger.info("Configuration loaded successfully from S3.")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {e}", exc_info=True)
        raise
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading config from S3: {e}", exc_info=True)
        raise
        sys.exit(1)

def get_hudi_options(table_name, database_name, primary_keys, precombine_field):
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': ','.join(primary_keys),
        'hoodie.datasource.write.precombine.field': precombine_field,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.partitionpath.field':"dt", 
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
        sys.exit(1)

def clean_column_names(df):
    try:
        new_columns = [col_name.strip().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").lower() for col_name in df.columns]
        for old_name, new_name in zip(df.columns, new_columns):
            df = df.withColumnRenamed(old_name, new_name)
        logger.info(f"Renamed columns: {dict(zip(df.columns, new_columns))}")
        return df
    except Exception as e:
        logger.error(f"Error cleaning column names: {e}")
        raise
        sys.exit(1)
    
def get_old_records(src_df, dest_df, joining_key):
    try:
        logger.info(f"inside get_old_record. Joining on key: {joining_key}")
        old_recs_df = src_df.join(dest_df, joining_key, 'inner').select(*dest_df)
        old_recs_df.printSchema()
        updated_old_recs_df = old_recs_df.withColumn("record_end_date", to_date(date_sub("current_date", 1), "yyyy-MM-dd")) \
                                    .withColumn("current_record_flag", lit(0)) \
                                    .withColumn("dt", to_date(date_sub("current_date", 1), "yyyy-MM-dd"))
        logger.info("Old records updated successfully.")
        return updated_old_recs_df
    except Exception as e:
        logger.error(f"Error getting old records: {e}")
        raise
        sys.exit(1)
    return updated_old_recs_df


def create_insert_df(df,Batch_date,delete_indicator_column):
    try:
        insert_df = df.withColumn("record_start_date", to_date(col(Batch_date), "yyyy-MM-dd")) \
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
        raise
        sys.exit(1)

def write_to_hudi(data_df, hudi_path, hudi_options):
    try:
        data_df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)
        logger.info(f"Successfully updated the Hudi table at {hudi_path}")
    except Exception as e:
        logger.error(f"Error during Hudi table update at {hudi_path}: {e}")
        sys.exit(1)
        
def load_hudi_table_data(glueContext, database_name, table_name,push_down_predicate):
    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name
            #push_down_predicate=push_down_predicate
        )
        dynamic_frame=dynamic_frame.toDF()
        logger.info("Loaded and filtered DynamicFrame from Glue Catalog.")
        return dynamic_frame
    except Exception as e:
        logger.error(f"Error loading and filtering Hudi table data from Glue catalog: {e}")
        raise
        sys.exit(1)

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


def process_rec_type(spark,rec_type,glueContext, rec_type_config,parquet_file_path,hudi_table_path, src_data_bkt):
    logger.info(f"Processing record type: {rec_type}")
    # parquet_file_path= f"s3://ddsl-raw-dq-dev1/batch_dq_recordlevel/accounting/good/{rec_type}/"
    # hudi_table_path = f"s3://ddsl-raw-dq-dev1/ddsl-hudi-table/{rec_type}/"
    primary_keys = rec_type_config.get("primary_keys", [])
    joining_key = rec_type_config.get("joining_key", [])
    table_name = f"{rec_type}"
    #table_name = rec_type + "_v3"
    database_name = rec_type_config.get("database_name", "")
    #database_name = "ddsl_account"
    precombine_field = rec_type_config.get("Batch_date", "")
    source_path = f's3://{src_data_bkt}/{rec_type_config.get("source_path", "")}' 
    print ('source_path ==== ', source_path)
    Batch_date=rec_type_config.get("Batch_date","")
    delete_indicator_column = rec_type_config.get("Delete_indicator_column", "")
    push_down_predicate="dt=current_date()-1"
    hudi_options = get_hudi_options(table_name, database_name, primary_keys, precombine_field)

    try:
        if check_s3_directory_exists(src_data_bkt, rec_type_config.get("source_path", "")):
            org_parquet_df = spark.read.format("parquet").load(source_path)
            logger.info(f"Loaded parquet data from {source_path}")
            org_parquet_df=org_parquet_df.drop("DataQualityRulesPass","DataQualityRulesFail","DataQualityRulesSkip","DataQualityEvaluationResult")
            org_parquet_df = clean_column_names(org_parquet_df)
            logger.info(f"org_parquet_df dataframe")
            src_insert_df = create_insert_df(org_parquet_df,Batch_date,delete_indicator_column)
            logger.info(f"src_insert_df dataframe")
            src_insert_df = src_insert_df.select([col(c).alias(c.lower()) for c in src_insert_df.columns])
            logger.info("Ensured column names are in lowercase")
            if hudi_table_exists(spark, hudi_table_path):
                #latest_hudi_df=load_hudi_table_data(glueContext, database_name, table_name,push_down_predicate)
                latest_hudi_df = spark.read.format("hudi").load(hudi_table_path).filter((col('current_record_flag') == 1)&(col('delete_flag') == 0)).select(*src_insert_df.columns)
                logger.info(f"Loaded existing Hudi table from {hudi_table_path}")
                old_records_update_df = get_old_records(src_insert_df, latest_hudi_df, joining_key)
                write_to_hudi(old_records_update_df, hudi_table_path, hudi_options)
                write_to_hudi(src_insert_df, hudi_table_path, hudi_options)
            else:
                logger.info("Hudi table does not exist. Creating a new table.")
                write_to_hudi(src_insert_df, hudi_table_path, hudi_options)
            logger.info(f"ETL job for record type {rec_type} completed successfully")
        else :
            print(f'File {rec_type_config.get("source_path", "")} does not exist in bucket {src_data_bkt}.')
    except Exception as e:
        logger.error(f"Unexpected error during ETL job for record type {rec_type}: {e}", exc_info=True)
        sys.exit(1)
        

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ['rec_type','env'])
    rec_type = args['rec_type']
    env = args['env']
    if env == 'dev' :
        raw_bkt = 'ddsl-raw-developer'
    else :
        raw_bkt = 'ddsl-raw-dev1'
        
    print ('raw_bkt = ', raw_bkt)
    bkt_params = read_s3_json(raw_bkt, 'job_config/bucket_config.json')
    
    parquet_file_path= f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq_recordlevel/accounting/good/{rec_type}/"
    hudi_table_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/ddsl-hudi-table/{rec_type}/"
    
    bucket_name = bkt_params[env]['RAW_DATA_BKT']
    config_file_key = 'job_config/config.json'
    config = read_config_from_s3(bucket_name, config_file_key)
    

    try:
        logger.info("Starting the ETL job")
        rec_type_param = config[rec_type]
        # Check if the input file exists
        process_rec_type(spark, rec_type,glueContext, rec_type_param, parquet_file_path,hudi_table_path, bkt_params[env]['DQ_DATA_BKT'])
        logger.info("ETL job completed successfully")
    except KeyError as e:
        logger.error(f"Configuration for record type '{rec_type}' not found: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during ETL job: {e}", exc_info=True)
        job.commit()
        sys.exit(1)


if __name__ == "__main__":
    main()
