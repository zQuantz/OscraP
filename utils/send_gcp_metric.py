from google.cloud import monitoring_v3
import time
import os

CLIENT = monitoring_v3.MetricServiceClient()

def send_gcp_metric(CONFIG, metric_name, metric_type, metric):

	series = monitoring_v3.types.TimeSeries()

	series.metric.type = f"custom.googleapis.com/{metric_name}"
	series.resource.type = 'gce_instance'
	series.resource.labels['instance_id'] = CONFIG['gcp_instance_id']
	series.resource.labels['zone'] = CONFIG['gcp_zone']

	point = series.points.add()
	setattr(point.value, metric_type, metric)
	now = time.time()
	point.interval.end_time.seconds = int(now)
	point.interval.end_time.nanos = int(
	    (now - point.interval.end_time.seconds) * 10**9)
	project_name = CLIENT.project_path(CONFIG['gcp_project_id'])
	CLIENT.create_time_series(project_name, [series])