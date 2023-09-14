from pyspark.sql.types import *
import unittest
from pyspark.sql import SparkSession
from src.assignment_1.utils import *

class MyTestCase(unittest.TestCase):
    spark = Spark_Session()
    def test_something(self):
        # dataframe schema for users
        userSchema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location", StringType(), True)
        ])

        # data given in the user file
        user_Data = [(101, "abc.123@gmail.com","hindi","mumbai"),
                     (102, "jhon@gmail.com", "english","usa"),
                     (103, "madan.44@gmail.com", "marathi","nagpur"),
                     (105, "sahil.55@gmail.com", "english","usa")
                     ]
        # creating two dataframes
        df_user = self.spark.createDataFrame(data=user_Data, schema=userSchema)

        transactionSchema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True)
        ])

        # data given in the transaction file
        transaction_Data = [(3300109, 1000009, 101, 500, "speaker"),
                            (3300102, 1000002, 102, 900, "keyboard"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300107, 1000007, 105, 66000, "laptop")
                            ]
        df_transaction = self.spark.createDataFrame(data=transaction_Data, schema=transactionSchema)

        expected_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location", StringType(), True),
        ])

        expected_data = [(3300109, 1000009, 101, 500, "speaker", 101, "abc.123@gmail.com", "hindi", "mumbai"),
                         (3300102, 1000002, 102, 900, "keyboard", 102, "jhon@gmail.com", "english", "usa"),
                         (3300103, 1000003, 103, 34000, "tv", 103, "madan.44@gmail.com", "marathi", "nagpur"),
                         (3300107, 1000007, 105, 66000, "laptop", 105, "sahil.55@gmail.com", "english", "usa")]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        df_transformed = join_dataframe(df_user, df_transaction)
        self.assertEqual(sorted(df_transformed.collect()), sorted(expected_df.collect()))

        # testing the count of unique locations where each product is sold.

        expected_schema1 = StructType([
            StructField("product_description", StringType(), True),
            StructField("location", StringType(), True),
            StructField("Count", LongType(), True)
        ])
        expected_data1 = [("tv", "nagpur", 1),
                          ("speaker", "mumbai", 1),
                          ("laptop", "usa", 1),
                          ("keyboard", "usa", 1)]
        expected_df1 = self.spark.createDataFrame(data=expected_data1, schema=expected_schema1)
        transformed_df1 = unique_loc(df_transformed)
        self.assertEqual(sorted(transformed_df1.collect()), sorted(expected_df1.collect()))

        # testing the products bought by each user.
        expected_schema2 = StructType([
           StructField("userid", IntegerType(), True),
           StructField("Bought_Products", ArrayType(StringType()), True)
        ])
        expected_data2 = [(101, ['speaker']), (102, ['keyboard']), (103, ['tv']), (105, ['laptop'])]
        expected_df2 = self.spark.createDataFrame(data=expected_data2, schema=expected_schema2)
        transformed_df2 = prod_user(df_transformed)
        self.assertEqual(sorted(transformed_df2.collect()), sorted(expected_df2.collect()))

        # testing the total spending by each user on each product
        expected_schema3 = StructType([
            StructField("userid", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("Total spending on each user", LongType(), True)
        ])
        expected_data3 = [(101, "speaker", 500), (102, "keyboard", 900), (103, "tv", 34000), (105, "laptop", 66000)]
        expected_df3 = self.spark.createDataFrame(data=expected_data3, schema=expected_schema3)
        transformed_df3 = total_spending(df_transformed)
        self.assertEqual(sorted(transformed_df3.collect()), sorted(expected_df3.collect()))


if __name__ == '__main__':
    unittest.main()

