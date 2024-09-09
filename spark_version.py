from pyspark.sql import SparkSession

spark = SparkSession.builder.master("lower").getOrCreate()
print(spark.sparkContext.version)