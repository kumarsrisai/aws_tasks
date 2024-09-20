import logging
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
import sys

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_config_from_s3(bucket_name, file_key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        config_content = response['Body'].read().decode('utf-8')
        config = json.loads(config_content)
        return config
    except Exception as e:
        logger.error(f"Error reading config from S3: {e}")
        raise

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
        'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
        'hoodie.cleaner.commits.retained': 0,
        'hoodie.cleaner.parallelism': 200,
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.datasource.hive_sync.use_jdbc': 'false',
        'hoodie.datasource.write.schema.evolution': 'true'
    }

def hudi_table_exists(spark, path: str) -> bool:
    try:
        hudi_df = spark.read.format("hudi").load(path)
        return hudi_df.count() > 0
    except Exception as e:
        logger.error(f"Error checking Hudi table existence: {e}")
        return False

def clean_column_names(df):
    new_columns = [col_name.strip().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").lower() for col_name in df.columns]
    for old_name, new_name in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_name, new_name)
    return df

def get_old_records(src_df, dest_df, joining_key):
    old_recs_df = src_df.join(dest_df, joining_key, 'inner').select(*dest_df.columns)
    updated_old_recs_df = old_recs_df.withColumn("record_end_date", to_date(lit(datetime.today().strftime('%Y-%m-%d')), "yyyy-MM-dd")) \
                                    .withColumn("current_record_flag", lit(0)) \
                                    .withColumn("dt", to_date(lit(datetime.today().strftime('%Y-%m-%d')), "yyyy-MM-dd"))
    return updated_old_recs_df

def create_insert_df(df, batch_date, delete_indicator_column):
    insert_df = df.withColumn("record_start_date", to_date(col(batch_date), "yyyy-MM-dd")) \
                  .withColumn("record_end_date", to_date(lit("9999-12-31"), "yyyy-MM-dd")) \
                  .withColumn("current_record_flag", lit(1)) \
                  .withColumn("dt", current_date())
    
    if delete_indicator_column:
        insert_df = insert_df.withColumn(
            "delete_flag",
            when(col(delete_indicator_column) == lit('D'), lit(1)).otherwise(lit(0))
        )
    else:
        insert_df = insert_df.withColumn("delete_flag", lit(0))
    
    return insert_df

def write_to_hudi(data_df, hudi_path, hudi_options):
    data_df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)

def check_s3_directory_exists(bucket: str, prefix: str) -> bool:
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    return 'Contents' in response or 'CommonPrefixes' in response

def process_rec_type(spark, rec_type, glueContext, rec_type_config, hudi_table_path, src_data_bkt):
    primary_keys = rec_type_config.get("primary_keys", [])
    joining_key = rec_type_config.get("joining_key", [])
    table_name = rec_type
    database_name = rec_type_config.get("database_name", "")
    precombine_field = rec_type_config.get("Batch_date", "")
    source_path = f's3://{src_data_bkt}/{rec_type_config.get("source_path", "")}'
    batch_date = rec_type_config.get("Batch_date", "")
    delete_indicator_column = rec_type_config.get("Delete_indicator_column", "")
    
    hudi_options = get_hudi_options(table_name, database_name, primary_keys, precombine_field)

    if check_s3_directory_exists(src_data_bkt, rec_type_config.get("source_path", "")):
        org_parquet_df = spark.read.format("parquet").load(source_path)
        org_parquet_df = clean_column_names(org_parquet_df)
        src_insert_df = create_insert_df(org_parquet_df, batch_date, delete_indicator_column)

        if hudi_table_exists(spark, hudi_table_path):
            latest_hudi_df = spark.read.format("hudi").load(hudi_table_path).filter(
                (col('current_record_flag') == 1) & (col('delete_flag') == 0)
            ).select(*src_insert_df.columns)
            old_records_update_df = get_old_records(src_insert_df, latest_hudi_df, joining_key)
            write_to_hudi(old_records_update_df, hudi_table_path, hudi_options)

        write_to_hudi(src_insert_df, hudi_table_path, hudi_options)
    else:
        logger.info(f'Source path {rec_type_config.get("source_path", "")} does not exist in bucket {src_data_bkt}.')

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    args = getResolvedOptions(sys.argv, ['rec_type', 'env', 'src_data_bkt', 'config_file'])
    
    rec_type = args['rec_type']
    env = args['env']
    src_data_bkt = args['src_data_bkt']
    config_file = args['config_file']
    
    raw_bkt = 'ddsl-raw-developer' if env == 'dev' else 'ddsl-raw-dev1'
    
    bkt_params = read_config_from_s3(raw_bkt, 'job_config/bucket_config.json')
    
    hudi_table_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/ddsl-hudi-table/{rec_type}/"
    bucket_name = bkt_params[env]['RAW_DATA_BKT']
    
    config = read_config_from_s3(bucket_name, config_file)

    try:
        rec_type_param = config[rec_type]
        process_rec_type(spark, rec_type, glueContext, rec_type_param, hudi_table_path, src_data_bkt)
    except KeyError as e:
        logger.error(f"Configuration for record type '{rec_type}' not found: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during ETL job: {e}")

if __name__ == "__main__":
    main()
