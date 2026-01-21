from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("CodespacesSparkTest")
        .master("local[*]")
        .getOrCreate()
    )

    print("Spark version:", spark.version)

    # Simple DataFrame
    df = spark.range(5)
    df.show()
      
    spark.stop()
    

if __name__ == "__main__":
    main()
