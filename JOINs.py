from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("JoinTypesExample") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()
# Sample data for Employees DataFrame
employee_data = [
    (1, "Siva", 101),
    (2, "Mani", 102),
    (3, "Sri", 103),
    (4, "Sruthi", None)
]
employee_columns = ["emp_id", "name", "dept_id"]

department_data = [
    (101, "HR"),
    (102, "Finance"),
    (104, "Marketing")
]
department_columns = ["dept_id", "dept_name"]

employees_df = spark.createDataFrame(employee_data, schema=employee_columns)
departments_df = spark.createDataFrame(department_data, schema=department_columns)

print("Inner Join Result:")
inner_join_df = employees_df.join(departments_df, employees_df.dept_id == departments_df.dept_id, "inner")
inner_join_df.show()

print("Left Outer Join Result:")
left_outer_join_df = employees_df.join(departments_df, employees_df.dept_id == departments_df.dept_id, "left")
left_outer_join_df.show()

print("Right Outer Join Result:")
right_outer_join_df = employees_df.join(departments_df, employees_df.dept_id == departments_df.dept_id, "right")
right_outer_join_df.show()

print("Full Outer Join Result:")
full_outer_join_df = employees_df.join(departments_df, employees_df.dept_id == departments_df.dept_id, "outer")
full_outer_join_df.show()

spark.stop()
