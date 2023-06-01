# Apache Spark
This repository currently contains code used for processing log files by turning them into CSV files. The idea is to use these log files for analysis and machine learning later on. The dataset being used is from [Kaggle](https://www.kaggle.com/datasets/omduggineni/loghub-hadoop-distributed-file-system-log-data) and contains a bunch of log files from a server that can be used for anomaly detection.

The setup used in this project is [Apache Spark](https://spark.apache.org/) running in multiple docker containers that create a cluster network where one container is the master node and the others are worker nodes. You can use the official docker container from [the apache software foundation](https://hub.docker.com/r/apache/spark) if you'd like to try it out yourself.

## Processing
There are currently 3 seperate files for processing where 2 are test files and 1 is used on all the log files. The data is being split into groups in each line and put into columns. These dataframes that are being created are then being put together into one big dataframe and afterwards being written to a CSV file. 

![CSV Structure after processing](/images/CSV_structure.png)