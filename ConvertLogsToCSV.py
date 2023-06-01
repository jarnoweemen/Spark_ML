from pyspark.sql import SparkSession
from pyspark.sql.functions import split, row_number
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.appName("ReadLogFile").master("spark://172.22.0.2:7077").config("spark.ui.port", "8080").config("spark.eventLog.enabled", "true") \
                            .config("spark.eventLog.dir", "./data/logs").config("spark.driver.port", "51810").config("spark.blockManager.port", "51814") \
                            .config("spark.executor.cores", "8").config("spark.executor.memory", "8128m").getOrCreate()

# Read the log file
log_df = spark.read.text("./data/HDFS_2/hadoop-hdfs-datanode-mesos-01.log")

# Split each line into words
split_df = log_df.select(
    split(log_df.value, " ")[0].alias('Date'),
    split(split(log_df.value, " ")[1], ",")[0].alias('Time'),
    split(split(log_df.value, " ")[1], ",")[1].alias('Pid'),
    split(log_df.value, " ")[2].alias('Level'),
    split(log_df.value, " ")[3].alias('Component'),
    split(log_df.value, " ", 5)[4].alias('Content'),
)

# Save DataFrame as a CSV file with a custom name
output_file_name = "custom_output"
output_csv_path = "./data/csv/" + output_file_name
split_df.coalesce(3).write.csv(output_csv_path, header=True)

# Stop the SparkSession
spark.stop()