from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re

# Create a SparkSession
spark = SparkSession.builder.appName("ReadLogFile").master("spark://172.22.0.2:7077").config("spark.ui.port", "8080").config("spark.eventLog.enabled", "true") \
                            .config("spark.eventLog.dir", "./data/logs").config("spark.driver.port", "51810").config("spark.blockManager.port", "51814") \
                            .config("spark.executor.cores", "8").config("spark.executor.memory", "8128m").getOrCreate()

log_file = './data/HDFS_2/hadoop-hdfs-datanode-mesos-02.log'
csv_file = './data/csv/custom'

# Define the regular expression pattern for extracting fields
log_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (\w+) (.*)'

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the log file as a DataFrame
log_df = spark.read.text(log_file)

# Extract the fields using regex and create new columns
log_df = log_df.select(
    F.regexp_extract('value', log_pattern, 1).alias('timestamp'),
    F.regexp_extract('value', log_pattern, 2).alias('level'),
    F.regexp_extract('value', log_pattern, 3).alias('message')
)

# Write the DataFrame to a CSV file
log_df.coalesce(3).write.csv(csv_file, header=True)