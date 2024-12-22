from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FraudDetectionBigQuery") \
    .config("spark.jars", "/path/to/spark-bigquery-with-dependencies.jar") \
    .getOrCreate()

# Load Processed Dataset
input_path = "gs://6893_waj/FraudLabelledData.parquet"
df = spark.read.parquet(input_path)

# BigQuery Configuration
project_id = "eecs6893-435023"
dataset_name = "frauddetection"
table_name = "processed_fraud_data"

# Write DataFrame to BigQuery
df.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{dataset_name}.{table_name}") \
    .option("temporaryGcsBucket", "6893_waj") \
    .mode("overwrite") \
    .save()

print(f"Data successfully written to BigQuery table: {project_id}:{dataset_name}.{table_name}")
