from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("ReadParquetExample") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()
parquet_file_path = "/Users/sivamanikandanselvakumar/IdeaProjects/untitled1/data/mtcars.parquet"

df = spark.read.parquet(parquet_file_path)

output_dir = "/Users/sivamanikandanselvakumar/IdeaProjects/untitled1/data/output"

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_dir)
