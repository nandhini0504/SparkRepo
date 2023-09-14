from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,LongType
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Spark_assignment_2").getOrCreate()
filepath = "C:/Users/NandhiniK/PycharmProjects/SparkRepo/Resource/ghtorrent-logs.txt"

#creating a function to load the file to a rdd.
def load_rdd(file_path):
    log_rdd = spark.sparkContext.textFile(file_path)
    return log_rdd

#creating a function to count the number of lines in the RDD.
def count_lines_rdd(log_rdd):
    line_count = log_rdd.count()
    return line_count

#creating a function to count the number of warning messages.
def count_warning_messages(log_rdd):
    warning_count = log_rdd.filter(lambda line: line.split(',')[0] == 'WARN').count()
    return warning_count

#creating a function to create a dataframe.
def create_df(filepath):
    schema = StructType([
        StructField("Logging_level", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Value", StringType(), True)
    ])
    log_df1 = spark.read.format("csv").schema(schema).load(filepath)
    log_df = log_df1.withColumn("Downloader_id",split("Value","--").getItem(0))\
                    .withColumn("Retrieval_Stage", split("Value", "--").getItem(1)) \
                    .withColumn("Client_name", split("Retrieval_Stage", ":").getItem(0)) \
                    .withColumn("Url", split("Retrieval_Stage", ":").getItem(1))\
                    .drop("Value","Retrieval_Stage")
    return log_df

#creating a function to count the repositories which were processed in total.
def count_processed_repositories(log_df):
    repository_count_df = log_df.groupBy("Client_name")\
                                 .count()\
                                 .filter(trim(col("Client_name")) == "api_client.rb")
    return repository_count_df

#creating a function to display which client did most HTTP requests.
def most_http_requests(log_df):
    most_http_requests_df = log_df.filter(trim(col("Url")).like("%request%"))\
                                  .groupBy(col("Client_name"))\
                                  .agg(count(col("Client_name")).alias("count of http request"))\
                                  .orderBy(col("count of http request").desc()) \
                                  .select("Client_name")
    return most_http_requests_df

#creating a function to display which client did most FAILED HTTP requests.
def most_failed_requests(log_df):
    failed_http_request_df = log_df.filter(trim(col("Url")).like("%Failed%"))\
                                   .groupBy(col("Client_name"))\
                                   .agg(count(col("Client_name")).alias("count of failed http request"))\
                                   .select("Client_name")
    return failed_http_request_df

# def most_active_hour(log_df):
#     active_hour_df = log_df.groupBy(col("Time")).count()
#     return active_hour_df

#creating a function to display the most active repository.
def count_most_active_repository(log_df):
    most_active_repo_df = log_df.groupBy(col("Client_name"))\
                                .agg(count(col("Client_name")).alias("Repo count"))\
                                .orderBy(col("Repo count").desc())\
                                .limit(1)\
                                .select(col("Client_name"))
    return most_active_repo_df