from src.assignment_1.utils import *
import logging
import sys
sys.path.append('C:/Users/NandhiniK/PycharmProjects/SparkRepo/src/assignment1_test.driver.py')

spark=Spark_Session()


df_user = user_data(spark)
df_user.show()

df_transaction=transaction_data(spark)
df_transaction.show()

df_join=join_dataframe(df_user,df_transaction)
df_join.show()

df_unique_location = unique_loc(df_join)       # Unique Cities where each product is sold
df_unique_location.show()

product_user = prod_user(df_join)      ## The products bought by each user
product_user.show()


amount = total_spending(df_join)     # Total amount of spending by each user on each product.
amount.show()
