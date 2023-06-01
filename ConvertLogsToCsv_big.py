from pyspark.sql import SparkSession
from pyspark.sql.functions import split, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

# Static values
NUMBERS_MISSING_LOG_FILES = [4, 12, 24]
NUMBER_LOG_FILES = 32 - len(NUMBERS_MISSING_LOG_FILES)
SPLIT_CSV_NUMBER = 3

# Create a SparkSession
spark = SparkSession.builder.appName("ReadLogFile").master("spark://172.22.0.2:7077").config("spark.ui.port", "8080").config("spark.eventLog.enabled", "true") \
                            .config("spark.eventLog.dir", "./data/logs").config("spark.driver.port", "51810").config("spark.blockManager.port", "51814").getOrCreate()

# Dictionary
log_dfs = {}

# Log files 4, 12 and 24 are missing
for i in range(32):
    if i + 1 in NUMBERS_MISSING_LOG_FILES:
        continue

    if i + 1 < 10:
        log_dfs[f"key{i + 1}"] = spark.read.text(f"./data/HDFS_2/hadoop-hdfs-datanode-mesos-0{i + 1}.log")
    else:
        log_dfs[f"key{i + 1}"] = spark.read.text(f"./data/HDFS_2/hadoop-hdfs-datanode-mesos-{i + 1}.log")

log_dfs_csv = {}

# Split into words and put df into new dictionary
for key, df in log_dfs.items():
    log_dfs_csv[key] = df.select(
    split(df.value, " ")[0].alias('Date'),
    split(split(df.value, " ")[1], ",")[0].alias('Time'),
    split(split(df.value, " ")[1], ",")[1].alias('Pid'),
    split(df.value, " ")[2].alias('Level'),
    split(df.value, " ")[3].alias('Component'),
    split(df.value, " ", 5)[4].alias('Content'),
    )

df_combined = None

# Combine all dataframes into a single dataframe
for key, df in log_dfs_csv.items():
    if df.exceptAll(log_dfs_csv["key1"]).count() == 0:
        df_combined = df

    df_combined = df_combined.union(df)

# Save DataFrame as a CSV file with a custom name
output_file_name = "custom_output"
output_csv_path = "./data/csv/" + output_file_name
df_combined.coalesce(NUMBER_LOG_FILES * SPLIT_CSV_NUMBER).write.csv(output_csv_path, header=True)

# Stop the SparkSession
spark.stop()