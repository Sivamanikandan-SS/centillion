from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col
from pyspark.sql.window import Window
from datetime import datetime, timedelta
#trans
spark = SparkSession \
    .builder \
    .appName("CustomerTransactions") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()
data = [

    (1, datetime.strptime("2023-11-01", "%Y-%m-%d").date(), 100.0),
    (1, datetime.strptime("2023-11-02", "%Y-%m-%d").date(), 150.0),
    (1, datetime.strptime("2023-11-03", "%Y-%m-%d").date(), 200.0),
    (1, datetime.strptime("2023-11-10", "%Y-%m-%d").date(), 120.0),
    (2, datetime.strptime("2023-11-01", "%Y-%m-%d").date(), 300.0),
    (2, datetime.strptime("2023-11-04", "%Y-%m-%d").date(), 250.0),
    (2, datetime.strptime("2023-11-06", "%Y-%m-%d").date(), 400.0)
]

columns = ["customer_id", "transaction_date", "amount"]
transactions = spark.createDataFrame(data, schema=columns)

cumulative_window = Window.partitionBy("customer_id").orderBy("transaction_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = transactions.withColumn("cumulative_amount", sum("amount").over(cumulative_window))

def calculate_rolling_avg(df):
    return df.withColumn(
        "rolling_avg_amount",
        avg("amount").over(Window.partitionBy("customer_id").orderBy("transaction_date").rowsBetween(-6, 0))
    )

result = calculate_rolling_avg(result)

result.show()
