from google.cloud import monitoring_v3
from const import CONFIG, DIR
import sys, os
import time

sys.path.append(f"{DIR}/../utils")
from send_gcp_metric import send_gcp_metric

if __name__ == '__main__':

	metric = len(os.listdir(f"{DIR}/news_data")) - 1
	send_gcp_metric(
		CONFIG,
		"rss_daily_collected_news_dfs_1d44b5268-56fc-44a7-a088-f3416f3fc91e",
		"int64_value",
		metric
	)
