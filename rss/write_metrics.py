from google.cloud import monitoring_v3
from const import DIR
import time
import os

PROJECT_ID = "dev-utility-270718"
RSS_NEWS_METRIC_ID = "custom.googleapis.com/rss_daily_collected_news_dfs_1d44b5268-56fc-44a7-a088-f3416f3fc91e"

CLIENT = monitoring_v3.MetricServiceClient()
PROJECT_NAME = CLIENT.project_path("dev-utility-270718")

def write_rss_daily_news_collected_dfs():

	series = monitoring_v3.types.TimeSeries()

	series.metric.type = RSS_NEWS_METRIC_ID
	series.resource.type = 'gce_instance'
	series.resource.labels['instance_id'] = '4589143820311923654'
	series.resource.labels['zone'] = 'northamerica-northeast1-b'

	point = series.points.add()
	point.value.int64_value = len(os.listdir(f"{DIR}/news_data")) - 1
	now = time.time()
	point.interval.end_time.seconds = int(now)
	point.interval.end_time.nanos = int(
	    (now - point.interval.end_time.seconds) * 10**9)
	CLIENT.create_time_series(PROJECT_NAME, [series])

if __name__ == '__main__':

	write_rss_daily_news_collected_dfs()
