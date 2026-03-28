# dependencies are installed

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

print("🔥 SCRIPT STARTED")

spark = SparkSession.builder \
    .appName("EmployeePipeline") \
    .config("spark.driver.extraClassPath", "/app/postgresql.jar") \
    .config("spark.executor.extraClassPath", "/app/postgresql.jar") \
    .getOrCreate()

# Load data : the csv loads the data
df = spark.read.csv("data/employees_raw.csv", header=True, inferSchema=True)
print("🔹 Raw Data Count:", df.count())

# Cleaning : filters everything
df = df.dropDuplicates(["employee_id"])
df = df.withColumn("email", lower(col("email")))
df = df.filter(col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$"))

df = df.withColumn("first_name", initcap(col("first_name"))) \
       .withColumn("last_name", initcap(col("last_name")))

df = df.withColumn(
    "salary",
    regexp_replace(col("salary"), "[$,]", "").cast("double")
)

df = df.filter(col("hire_date") <= current_date())

# Transform : converts into useful data insights
df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
df = df.withColumn("email_domain", split(col("email"), "@").getItem(1))
df = df.withColumn("age", year(current_date()) - year(col("birth_date")))
df = df.withColumn("tenure_years", year(current_date()) - year(col("hire_date")))

df = df.withColumn(
    "salary_band",
    when(col("salary") < 50000, "Junior")
    .when(col("salary") <= 80000, "Mid")
    .otherwise("Senior")
)

print("✅ Clean Data Count:", df.count())

# Writes to PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/employee_db") \
    .option("dbtable", "employees_clean") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("🎉 DATA LOADED INTO POSTGRES SUCCESSFULLY")