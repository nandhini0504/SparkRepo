from src.assignment_2.utils import *
from Resource import *
import unittest

class SparkAssignment2Test(unittest.TestCase):

#creating a test function to test count_lines_rdd()
    def test_count_lines_rdd(self):
        data = [("INFO", "2017-03-22T20:11:49+00:00",
                 "ghtorrent-31 -- ghtorrent.rb: Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341"),
                ("DEBUG", "2017-03-23T11:15:14+00:00",
                 "ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
                ("DEBUG", "2017-03-22T20:15:48+00:00",
                 "ghtorrent-35 -- ghtorrent.rb: Parent af8451e16e077f7c6cae3f98bf43bffaca562f88 for commit 2ef393531a3cfbecc69f17d2cedcc95662fae1e6 exists"),
                ("DEBUG", "2017-03-24T12:29:50+00:00",
                 "ghtorrent-49 -- ghtorrent.rb: Parent cf060bf3b789ac6391b2f7c1cdc34191c2bc773d for commit 8c924c1115e1abddcaddc27c6e7fd5806583ea90 exists")]
        rdd = spark.sparkContext.parallelize(data)
        actual_output = count_lines_rdd(rdd)
        expected_output = 4
        self.assertEqual(actual_output, expected_output)

    #creating a test function test count_warning_messages()
    def test_count_warning_rdd(self):
        data = [("INFO,2017-03-22T20:11:49+00:00,ghtorrent-31 -- ghtorrent.rb: Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341"),
                ("DEBUG,2017-03-23T11:15:14+00:00,ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
                ("WARN,2017-03-23T16:04:59+00:00,ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 2914"),
                ("DEBUG,2017-03-22T20:15:48+00:00,ghtorrent-35 -- ghtorrent.rb: Parent af8451e16e077f7c6cae3f98bf43bffaca562f88 for commit 2ef393531a3cfbecc69f17d2cedcc95662fae1e6 exists"),
                ("DEBUG,2017-03-24T12:29:50+00:00,ghtorrent-49 -- ghtorrent.rb: Parent cf060bf3b789ac6391b2f7c1cdc34191c2bc773d for commit 8c924c1115e1abddcaddc27c6e7fd5806583ea90 exists"),
                ("WARN,2017-03-23T10:33:43+00:00,ghtorrent-23 -- ghtorrent.rb: Not a valid email address: greenkeeper[bot]"),
                ("WARN,2017-03-23T11:17:27+00:00,ghtorrent-39 -- ghtorrent.rb: Transaction failed (364 ms)")
                ]
        rdd = spark.sparkContext.parallelize(data)
        actual_output = count_warning_messages(rdd)
        expected_output = 3
        self.assertEqual(actual_output, expected_output)

    #creating a test function to count the repositories which were processed in total.
    def test_count_processed_repositories(self):
        schema = StructType([
            StructField("Logging_level", StringType(), True),
            StructField("Time", StringType(), True),
            StructField("Value", StringType(), True)
                            ])
        data = [("INFO","2017-03-22T20:11:49+00:00","ghtorrent-31 -- ghtorrent.rb: Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341"),
                ("DEBUG", "2017-03-23T11:15:14+00:00","ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
                ("DEBUG","2017-03-22T20:15:48+00:00","ghtorrent-35 -- ghtorrent.rb: Parent af8451e16e077f7c6cae3f98bf43bffaca562f88 for commit 2ef393531a3cfbecc69f17d2cedcc95662fae1e6 exists"),
                ("INFO","2017-03-23T11:06:56+00:00","ghtorrent-14 -- api_client.rb: Successful request. URL: https://api.github.com/repos/DuoSoftware/DVP-MongoModels/commits/1ad657d96ea32bc504d6b9fff419568c39d6cb69?per_page=100, Remaining: 3597, Total: 104 ms"),
                ("WARN","2017-03-24T01:03:55+00:00","ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 2267")
                ]
        log_df1 = spark.createDataFrame(data=data , schema = schema)
        log_df = log_df1.withColumn("Downloader_id", split("Value", "--").getItem(0)) \
                           .withColumn("Retrieval_Stage", split("Value", "--").getItem(1)) \
                           .withColumn("Client_name", split("Retrieval_Stage", ":").getItem(0)) \
                           .withColumn("Url", split("Retrieval_Stage", ":").getItem(1)) \
                           .drop("Value", "Retrieval_Stage")
        actual_df = count_processed_repositories(log_df)
        expected_schema = StructType([
            StructField("Client_name", StringType(), True),
            StructField("count", LongType(), True)
        ])
        expected_data = [(" api_client.rb",2)]
        expected_df = spark.createDataFrame(data = expected_data,schema = expected_schema)
        self.assertEqual(actual_df.collect(),expected_df.collect())

    #creating a test function to display which client did most http request.
    def test_most_http_requests(self):
        schema = StructType([
        StructField("Logging_level", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Value", StringType(), True)
                            ])
        data = [("INFO", "2017-03-22T20:11:49+00:00",
         "ghtorrent-31 -- ghtorrent.rb: Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341"),
        ("DEBUG", "2017-03-23T11:15:14+00:00",
         "ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
        ("WARN","2017-03-24T01:03:55+00:00","ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 2267"),
        ("INFO","2017-03-23T10:08:17+00:00","ghtorrent-34 -- api_client.rb: Successful request. URL: https://api.github.com/repos/olivia-bengtsson/dinnerplanner-html/pulls?page=1&per_page=100, Remaining: 3210, Total: 74 ms"),
        ("INFO","2017-03-23T11:17:41+00:00","ghtorrent-14 -- api_client.rb: Successful request. URL: https://api.github.com/repos/IoTTV/iotivity-constrained/labels?per_page=100, Remaining: 1457, Total: 62 ms"),
        ("WARN","2017-03-23T20:06:13+00:00","ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 1878")
                ]
        log_df1 = spark.createDataFrame(data=data, schema=schema)
        log_df = log_df1.withColumn("Downloader_id", split("Value", "--").getItem(0)) \
            .withColumn("Retrieval_Stage", split("Value", "--").getItem(1)) \
            .withColumn("Client_name", split("Retrieval_Stage", ":").getItem(0)) \
            .withColumn("Url", split("Retrieval_Stage", ":").getItem(1)) \
            .drop("Value", "Retrieval_Stage")
        actual_df = most_http_requests(log_df)
        expected_schema = StructType([
            StructField("Client_name", StringType(), True)
        ])
        expected_data = [(" api_client.rb",)]
        expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(actual_df.collect(),expected_df.collect())

    def test_most_failed_requests(self):
        schema = StructType([
            StructField("Logging_level", StringType(), True),
            StructField("Time", StringType(), True),
            StructField("Value", StringType(), True)
        ])
        data = [("INFO", "2017-03-22T20:11:49+00:00",
                 "ghtorrent-31 -- ghtorrent.rb: Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341"),
                ("DEBUG", "2017-03-23T11:15:14+00:00",
                 "ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
                ("WARN", "2017-03-24T01:03:55+00:00",
                 "ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 2267"),
                ("INFO", "2017-03-23T10:08:17+00:00",
                 "ghtorrent-34 -- api_client.rb: Successful request. URL: https://api.github.com/repos/olivia-bengtsson/dinnerplanner-html/pulls?page=1&per_page=100, Remaining: 3210, Total: 74 ms"),
                ("INFO", "2017-03-23T11:17:41+00:00",
                 "ghtorrent-14 -- api_client.rb: Successful request. URL: https://api.github.com/repos/IoTTV/iotivity-constrained/labels?per_page=100, Remaining: 1457, Total: 62 ms"),
                ("WARN", "2017-03-23T20:06:13+00:00",
                 "ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 1878")
                ]
        log_df1 = spark.createDataFrame(data=data, schema=schema)
        log_df = log_df1.withColumn("Downloader_id", split("Value", "--").getItem(0)) \
            .withColumn("Retrieval_Stage", split("Value", "--").getItem(1)) \
            .withColumn("Client_name", split("Retrieval_Stage", ":").getItem(0)) \
            .withColumn("Url", split("Retrieval_Stage", ":").getItem(1)) \
            .drop("Value", "Retrieval_Stage")
        actual_df = most_failed_requests(log_df)
        expected_schema = StructType([
            StructField("Client_name", StringType(), True)
        ])
        expected_data = [(" api_client.rb",)]
        expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(actual_df.collect(), expected_df.collect())

    def test_count_most_active_repository(self):
        schema = StructType([
            StructField("Logging_level", StringType(), True),
            StructField("Time", StringType(), True),
            StructField("Value", StringType(), True)
            ])
        data = [("INFO","2017-03-22T20:11:49+00:00","ghtorrent-31 -- ghtorrent.rb: Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341"),
            ("DEBUG","2017-03-23T11:15:14+00:00","ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
            ("DEBUG","2017-03-22T20:15:48+00:00","ghtorrent-35 -- ghtorrent.rb: Parent af8451e16e077f7c6cae3f98bf43bffaca562f88 for commit 2ef393531a3cfbecc69f17d2cedcc95662fae1e6 exists"),
            ("DEBUG","2017-03-24T12:29:50+00:00","ghtorrent-49 -- ghtorrent.rb: Parent cf060bf3b789ac6391b2f7c1cdc34191c2bc773d for commit 8c924c1115e1abddcaddc27c6e7fd5806583ea90 exists"),
            ("DEBUG","2017-03-24T10:52:47+00:00","ghtorrent-50 -- ghtorrent.rb: Repo alyuev/urantia-study-edition exists"),
            ("INFO","2017-03-23T09:11:17+00:00","ghtorrent-5 -- api_client.rb: Successful request. URL: https://api.github.com/repos/javier-serrano/web-ui-a2?per_page=100, Remaining: 3381, Total: 153 ms"),
            ("INFO","2017-03-23T09:22:56+00:00","ghtorrent-1 -- api_client.rb: Successful request. URL: https://api.github.com/repos/llyp618/move.js/labels/invalid, Remaining: 2040, Total: 68 ms"),
            ("INFO","2017-03-23T11:12:36+00:00","ghtorrent-32 -- api_client.rb: Successful request. URL: https://api.github.com/repos/mdmahamodur2013/EcomAngularRawPhp/pulls?page=1&per_page=100, Remaining: 2348, Total: 62 ms")
                ]
        log_df1 = spark.createDataFrame(data=data, schema=schema)
        log_df = log_df1.withColumn("Downloader_id", split("Value", "--").getItem(0)) \
            .withColumn("Retrieval_Stage", split("Value", "--").getItem(1)) \
            .withColumn("Client_name", split("Retrieval_Stage", ":").getItem(0)) \
            .withColumn("Url", split("Retrieval_Stage", ":").getItem(1)) \
            .drop("Value", "Retrieval_Stage")
        actual_df = count_most_active_repository(log_df)
        expected_schema = StructType([
            StructField("Client_name", StringType(), True)
        ])
        expected_data = [(" ghtorrent.rb",)]
        expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(actual_df.collect(),expected_df.collect())




if __name__ == '__main__':
    unittest.main()