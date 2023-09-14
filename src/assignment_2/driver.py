from src.assignment_2.utils import *
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../../logs/assignment2.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
file_path = "../../resource/ghtorrent-logs.txt"

log_rdd = load_rdd(file_path)
# log_rdd.foreach(print)

line_count = count_lines_rdd(log_rdd)
logging.info(" Number of lines in rdd:%d" %line_count)

warning_count = count_warning_messages(log_rdd)
logging.info("Number of warnings in rdd : %s" %warning_count)

df_log = create_df(filepath)
logging.info("Dataframe created from rdd")
df_log.show(10,truncate = False)

df_repository_count = count_processed_repositories(df_log)
logging.info("Api Repositories that were processed in total")
df_repository_count.show()

df_most_http_requests = most_http_requests(df_log)
logging.info("Client with most http request")
df_most_http_requests.show(1,truncate=False)

df_most_failed_requests_client = most_failed_requests(df_log)
logging.info("Client with most failed http request")
df_most_failed_requests_client.show(truncate=False)

# df_active_hour_df = most_active_hour(df_log)
# df_active_hour_df.show(truncate=False)

df_most_active_repo_count = count_most_active_repository(df_log)
logging.info("Most active repository")
df_most_active_repo_count.show(truncate=False)