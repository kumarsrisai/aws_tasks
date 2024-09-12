import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import json
import boto3
from botocore.exceptions import NoCredentialsError
from urllib.parse import urlparse
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import col, to_date

# Initialize Glue context and Spark session
glueContext = GlueContext(SparkContext.getOrCreate())  # Use existing SparkContext
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['env'])
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

if env == 'dev':
    raw_bkt = 'ddsl-raw-developer'
else:
    raw_bkt = 'ddsl-dq-developer'

print('raw_bkt = ', raw_bkt)
bkt_params = read_s3_json(raw_bkt, 'job_config/bucket_config.json')

# Updated to use rec_type_9000.txt
input_file_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq/good/rec_type_9000.txt"
rules_file_path = f"s3://{bkt_params[env]['RAW_DATA_BKT']}/job_config/ruleset.json"
failed_records_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq/bad/rec_type_9000/"
passed_records_path = f"s3://{bkt_params[env]['DQ_DATA_BKT']}/batch_dq/good/rec_type_9000/"

# Read the input file from S3 directly into a Spark DataFrame
spark_df = spark.read.csv(
    input_file_path,
    sep='|',
    header=False,
    quote='"',
    inferSchema=True
)

# Load parameters from S3
column_details = read_s3_json(bkt_params[env]['RAW_DATA_BKT'], 'job_config/columns.json')
rec_type_columns = column_details['9000']  # Hardcoded to use rec_type_9000

# Rename columns based on rec_type_9000
for i in range(len(rec_type_columns)):
    spark_df = spark_df.withColumnRenamed("_c{}".format(i), rec_type_columns[i])

# Drop the last column
last_column = spark_df.columns[-1]
spark_df = spark_df.drop(last_column)

# Identify columns that include '_DATE' and convert them to DateType
date_columns = [c for c in spark_df.columns if '_DATE' in c]
for date_col in date_columns:
    spark_df = spark_df.withColumn(date_col, to_date(col(date_col), 'yyyyMMdd'))

# Extract bucket and key from the rules_file_path
parsed_url = urlparse(rules_file_path)
rules_bucket = parsed_url.netloc
rules_key = parsed_url.path.lstrip('/')

# Load ruleset from S3 using the rules_file_path
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

all_ruleset_str = read_s3_text(rules_bucket, rules_key)
all_ruleset_dict = json.loads(all_ruleset_str)
rec_type_ruleset = all_ruleset_dict['9000']  # Hardcoded to use rec_type_9000

rec_type_ruleset = all_ruleset_dict['9000'].replace("'", '"')
ruleset = f"""Rules = {rec_type_ruleset}"""
print(ruleset)

# Convert DataFrame to DynamicFrame for Glue
rule_AmazonS3 = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_name")


# # data quality evaluation 
# evaluation_result = EvaluateDataQuality().process_rows(
#     frame=rule_AmazonS3,
#     ruleset=ruleset,
#     publishing_options={
#         "dataQualityEvaluationContext": "EvaluateDataQuality",
#         "enableDataQualityCloudWatchMetrics": False,
#         "enableDataQualityResultsPublishing": False
#     },
#     additional_options={"observations.scope": "ALL", "performanceTuning.caching": "CACHE_NOTHING"}
# )

# # Extract rule outcomes
# rule_outcomes = SelectFromCollection.apply(dfc=evaluation_result, key="ruleOutcomes", transformation_ctx="ruleOutcomes")
# # rule_outcomes.toDF().show(truncate=False)



# rowLevelOutcomes = SelectFromCollection.apply(
#     dfc=evaluation_result,
#     key="rowLevelOutcomes",
#     transformation_ctx="rowLevelOutcomes",
# )

# rowLevelOutcomes_df = rowLevelOutcomes.toDF() # Convert Glue DynamicFrame to SparkSQL DataFrame
# rowLevelOutcomes_df_passed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Passed") # Filter only the Passed records
# rowLevelOutcomes_df_failed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Failed") # Review the Failed records 

# # Convert DataFrames back to DynamicFrames
# dynamic_frame_passed = DynamicFrame.fromDF(rowLevelOutcomes_df_passed, glueContext, "dynamic_frame_passed")
# dynamic_frame_failed = DynamicFrame.fromDF(rowLevelOutcomes_df_failed, glueContext, "dynamic_frame_failed")



# # Write Passed records to S3
# glueContext.write_dynamic_frame.from_options(
#     frame=dynamic_frame_passed,
#     connection_type="s3",
#     connection_options={"path": passed_records_path},
#     format="parquet"
# )




# # write Failed records to S3
# glueContext.write_dynamic_frame.from_options(
#     frame=dynamic_frame_failed,
#     connection_type="s3",
#     connection_options={"path": failed_records_path},
#     format="parquet"
# )


# job.commit()



# import logging
# import json
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from datetime import datetime
# import sys
# from functools import reduce


# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# def read_config_from_s3(bucket_name, file_key):
#     s3_client = boto3.client('s3')
#     try:
#         response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
#         config_content = response['Body'].read().decode('utf-8')
#         logger.info(f"Configuration content: {config_content}")  # Debug print
#         config = json.loads(config_content)
#         logger.info("Configuration loaded successfully from S3.")
#         return config
#     except json.JSONDecodeError as e:
#         logger.error(f"JSON decoding error: {e}", exc_info=True)
#         raise
#         sys.exit(1)
#     except Exception as e:
#         logger.error(f"Error reading config from S3: {e}", exc_info=True)
#         raise
#         sys.exit(1)

# def get_hudi_options(table_name, database_name, primary_keys, precombine_field):
#     return {
#         'hoodie.table.name': table_name,
#         'hoodie.datasource.write.recordkey.field': ','.join(primary_keys),
#         'hoodie.datasource.write.precombine.field': precombine_field,
#         'hoodie.datasource.write.operation': 'upsert',
#         'hoodie.datasource.write.table.name': table_name,
#         'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
#         'hoodie.datasource.write.partitionpath.field':"dt", 
#         'hoodie.datasource.hive_sync.enable': 'true',
#         'hoodie.datasource.hive_sync.database': database_name,
#         'hoodie.datasource.hive_sync.table': table_name,
#         'hoodie.datasource.write.hive_style_partitioning': 'true',
#         'hoodie.datasource.meta_sync.condition.sync': 'true',
#         'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
#         'hoodie.cleaner.commits.retained': 0,
#         'hoodie.cleaner.commits.retained.max': 7,
#         'hoodie.cleaner.parallelism': 200,
#         'hoodie.datasource.hive_sync.mode': 'hms',
#         'hoodie.datasource.hive_sync.use_jdbc': 'false',
#         'hoodie.datasource.write.schema.evolution': 'true'
#     }

# def hudi_table_exists(spark, path: str) -> bool:
#     try:
#         hudi_df = spark.read.format("hudi").load(path)
#         exists = hudi_df.count() > 0
#         logger.info(f"Hudi table existence check at path {path}: {'Exists' if exists else 'Does not exist'}")
#         return exists
#     except Exception as e:
#         logger.error(f"Error checking Hudi table existence at {path}: {e}")
#         return False
#         #sys.exit(1)

# def clean_column_names(df):
#     try:
#         new_columns = [col_name.strip().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").lower() for col_name in df.columns]
#         for old_name, new_name in zip(df.columns, new_columns):
#             df = df.withColumnRenamed(old_name, new_name)
#         logger.info(f"Renamed columns: {dict(zip(df.columns, new_columns))}")
#         return df
#     except Exception as e:
#         logger.error(f"Error cleaning column names: {e}")
#         raise
#         #sys.exit(1)
    
# def write_to_hudi(data_df, hudi_path, hudi_options):
#     try:
#         data_df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)
#         logger.info(f"Successfully updated the Hudi table at {hudi_path}")
#     except Exception as e:
#         logger.error(f"Error during Hudi table update at {hudi_path}: {e}")
#         #sys.exit(1)

# def process_rec_type(spark,rec_type, rec_type_config):
#     logger.info(f"Processing record type: {rec_type}")
#     parquet_file_path = f"s3://ddsl-dq-developer/batch_dq/good/{rec_type}/"
#     transpose_history_hudi_table_path = f"s3://ddsl-dq-developer/history_hudi_path_v2/{rec_type}/"
#     primary_keys = rec_type_config.get("primary_keys", [])
#     joining_key = rec_type_config.get("joining_key", [])
#     database_name = rec_type_config.get("database_name", "")
#     precombine_field = rec_type_config.get("Batch_date", "")
#     path = rec_type_config.get("source_path", "") 
#     Batch_date=rec_type_config.get("Batch_date","")
#     delete_indicator_column = rec_type_config.get("Delete_indicator_column", "")
#     history_table_name = "transpose_history_" + rec_type
#     logger.info(f"history_table_name {history_table_name}")
#     history_hudi_options_df = get_hudi_options(history_table_name, database_name, primary_keys, precombine_field)
#     try:
        
#         org_parquet_df = spark.read.format("parquet").load(path)
#         logger.info(f"Loaded parquet data from {parquet_file_path}")
#         org_parquet_df=org_parquet_df.drop("DataQualityRulesPass","DataQualityRulesFail","DataQualityRulesSkip","DataQualityEvaluationResult")
#         src_insert_df = clean_column_names(org_parquet_df)
#         logger.info(f"org_parquet_df dataframe")
#         src_insert_df =src_insert_df.select([col(c).alias(c.lower()) for c in src_insert_df.columns])
#         src_insert_df=src_insert_df.withColumn("dt", current_date())
#         logger.info("Ensured column names are in lowercase")
#         if hudi_table_exists(spark, transpose_history_hudi_table_path):
#             logger.info("transpose_history_hudi_table exist. appending the records into the table.")
#             write_to_hudi(src_insert_df, transpose_history_hudi_table_path, history_hudi_options_df)
#             logger.info("ETL job for record type {rec_type} completed successfully")
#         else:
#             logger.info("Hudi table does not exist. Creating a new table for transpose history.")
#             write_to_hudi(src_insert_df, transpose_history_hudi_table_path, history_hudi_options_df)
#             logger.info("ETL job for record type {rec_type} completed successfully")
#     except Exception as e:
#         logger.error(f"Unexpected error during ETL job for record type {rec_type}: {e}", exc_info=True)
#         #sys.exit(1)

# def main():
#     bucket_name = 'ddsl-dq-cg-vl'
#     config_file_key = 'transpose_config.json'
#     config = read_config_from_s3(bucket_name, config_file_key)
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     args = getResolvedOptions(sys.argv, ['rec_type'])
#     rec_type = args['rec_type']

#     try:
#         logger.info("Starting the ETL job")
#         rec_type_param = config[rec_type]
#         process_rec_type(spark, rec_type, rec_type_param)
#         logger.info("ETL job completed successfully")
#     except KeyError as e:
#         logger.error(f"Configuration for record type '{rec_type}' not found: {e}", exc_info=True)
#         #sys.exit(1)
#     except Exception as e:
#         logger.error(f"Unexpected error during ETL job: {e}", exc_info=True)
#         job.commit()
#         #sys.exit(1)


# if __name__ == "__main__":
#     main()

