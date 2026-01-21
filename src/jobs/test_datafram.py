from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Read Uploaded CSV")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sales_data_first.csv")
)

df.show()
spark.stop()
