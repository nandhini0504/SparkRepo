from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

# creating a function to create spark Session
def Spark_Session():
    spark = SparkSession.builder.appName("assignment1_test").getOrCreate()
    return spark

# creating a function to create data frame for user.csv file

def user_data(spark):
    df_user = spark.read.csv("C:/Users/NandhiniK/PycharmProjects/SparkRepo/Resource/user.csv", header="true", inferSchema="true")
    return df_user

# creating a function to create data frame for transaction.csv file
def transaction_data(spark):
    df_transaction= spark.read.csv("C:/Users/NandhiniK/PycharmProjects/SparkRepo/Resource/transaction.csv", header="true", inferSchema="true")
    return df_transaction

# Using inner join to join user and transaction dataframes
def join_dataframe(df_user, df_transaction):        # An inner join returns only the rows that have matching values in both data frames.
    df_join = df_transaction.join(df_user, df_user.user_id == df_transaction.userid, "inner")
    return df_join

#creating a function that counts unique locations where each product is sold.
def unique_loc(df_join):
    df_unique_locations= df_join.groupBy("product_description","location").agg(countDistinct("location"))
    return df_unique_locations

# The products bought by each user (list of products )
def prod_user(df_join):
    product_user = df_join.groupBy('userid').agg(collect_list("product_description"))      #collect_set This function collects all unique values from that column  within the group and store them in array or set.
    return product_user

#creating a function that calculates total spending done by each user on each product.
def total_spending(df_join):
    amount = df_join.groupBy('userid','product_description').agg(sum('price'))
    return amount