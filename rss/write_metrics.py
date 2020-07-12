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
		"rss_daily_item_counter",
		"int64_value",
		metric
	)
