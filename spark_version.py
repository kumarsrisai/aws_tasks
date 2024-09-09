from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()
print(spark.sparkContext.version)