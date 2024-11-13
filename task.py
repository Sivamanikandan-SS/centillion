from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import datetime
#ecommerce
spark = SparkSession \
    .builder \
    .appName("EcommerceTransactions") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", DateType(), True)
])

data = [
    ("T001", "U001", "P001", "Electronics", 120.0, datetime.strptime("2023-11-01", "%Y-%m-%d").date()),
    ("T002", "U002", "P002", "Books", 30.0, datetime.strptime("2023-11-02", "%Y-%m-%d").date()),
    ("T003", "U001", "P003", "Electronics", 80.0, datetime.strptime("2023-11-03", "%Y-%m-%d").date()),
    ("T004", "U001", "P004", "Books", 20.0, datetime.strptime("2023-11-04", "%Y-%m-%d").date()),
    ("T005", "U002", "P005", "Clothing", 50.0, datetime.strptime("2023-11-05", "%Y-%m-%d").date()),
    ("T006", "U003", "P006", "Electronics", 200.0, datetime.strptime("2023-11-06", "%Y-%m-%d").date()),
    ("T007", "U002", "P007", "Electronics", 40.0, datetime.strptime("2023-11-07", "%Y-%m-%d").date())
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Step 1: Calculate the total and average spending per user
user_spending = df.groupBy("user_id") \
    .agg(sum("amount").alias("total_spent"),
         avg("amount").alias("avg_transaction"))

# Step 2: Find the most frequently purchased category per user
category_counts = df.groupBy("user_id", "category") \
    .agg(count("category").alias("category_count"))

# Use window function to find the category with the highest count per user
window = Window.partitionBy("user_id").orderBy(col("category_count").desc())
favorite_category_df = category_counts \
    .withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1) \
    .select("user_id", "category")

# Step 3: Join the total spending and favorite category DataFrames
result = user_spending.join(favorite_category_df, on="user_id") \
    .select("user_id", "total_spent", "avg_transaction", col("category").alias("favorite_category"))

result.show()