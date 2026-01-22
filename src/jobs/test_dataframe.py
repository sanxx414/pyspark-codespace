from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Spark Hive HDFS Test")
    .master("local[*]")
    .enableHiveSupport()  
    .getOrCreate()
)

# Read from local
df = spark.read.option("header", "true").csv(
    "/workspaces/pyspark-codespace/data/sales_data_first.csv"
)

df.show()

# Create Hive table
df.write.mode("overwrite").saveAsTable("sales_data")

spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM sales_data").show()

spark.stop()
