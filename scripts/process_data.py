from pyspark.sql import SparkSession
import boto3

def process_data(file_path, redshift_table, redshift_credentials):
    spark = SparkSession.builder \
        .appName("API Data Processing") \
        .getOrCreate()

    # Read data from JSON file
    df = spark.read.json(file_path)

    # Perform data processing with PySpark (example transformation)
    df_processed = df.filter(df['value'] > 100)

    # Write processed data to Amazon Redshift
    df_processed.write \
        .format("jdbc") \
        .option("url", redshift_credentials['url']) \
        .option("dbtable", redshift_table) \
        .option("user", redshift_credentials['user']) \
        .option("password", redshift_credentials['password']) \
        .save()

if __name__ == "__main__":
    file_path = "/path/to/save/data.json"
    redshift_table = "public.processed_data"
    redshift_credentials = {
        "url": "jdbc:redshift://your-redshift-cluster:5439/your-database",
        "user": "your-username",
        "password": "your-password"
    }
    process_data(file_path, redshift_table, redshift_credentials)
