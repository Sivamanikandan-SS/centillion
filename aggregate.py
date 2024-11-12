from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, min, max


spark = SparkSession \
    .builder \
    .appName("AggregateExample") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()
data = [
    ("Siva ", "HR", 3000),
    ("Selva", "HR", 4000),
    ("Sriram", "Engineering", 5000),
    ("Super", "Engineering", 4500),
    ("Siva", "Finance", 5500)
]
columns = ["name", "department", "salary"]

df = spark.createDataFrame(data, schema=columns)

df.select(
    count("*").alias("total_employees"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("average_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
).show()

spark.stop()
