# SparkRepo

## Spark Assignment1 ##

- There are two files user.csv and transactions.csv
- User file has fields User_ID,EmailID,NativeLanguage,Location.
- Transaction file has fields Transaction_ID,Product_ID,UserID,Price,Product_Description
- Firstly we need to count the unique locations where each product is sold.
- Secondly finding out products bought by each user.
- Third calculating the total spending done by each user on each product.

## Spark Assignment2 ##

- There is a log file ghtorrent log file which has following data- 1.Logging level, one of DEBUG, INFO, WARN, ERROR (separated by ,) 2.A timestamp (separated by ,) 3.The downloader id, denoting the downloader instance (separated by --) 4.The retrieval stage, denoted by the Ruby class name, one of: • event_processing • ght_data_retrieval • api_client • retriever • ghtorrent
- First we are writing a function to the load the file into RDD.
- Writing a function to count the number of lines in RDD.
- Writing a function to count the number of Warning messages.
- Writing a function to count the total number of api_client repositories that were processed.
- Writing a function to display which client did most http requests.
- Writing a function to display which client did most failed http requests.
- Writing a function to display most active repository.
