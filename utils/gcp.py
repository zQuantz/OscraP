from google.cloud import monitoring_v3

from dummy_logger import DummyLogger
from google.cloud import storage
from hashlib import sha256

import json
import time
import os

###################################################################################################

METRIC_CLIENT = monitoring_v3.MetricServiceClient()
STORAGE_CLIENT = storage.Client()

###################################################################################################

def send_to_bucket(bucket_prefix, bucket_name, filename, filepath, logger=None):

	if not logger:
		logger = DummyLogger()

	max_tries = 5
	storage_attempts = 0

	fullname = f"{filepath}/{filename}"

	while storage_attempts < max_tries:

		try:

			STORAGE_CLIENT = storage.Client()
			bucket = STORAGE_CLIENT.bucket(bucket_name)

			blob = bucket.blob(f"{bucket_prefix}/{filename}")
			blob.upload_from_filename(fullname)
			
			with open(fullname, "rb") as file:
				local_hash = sha256(file.read()).hexdigest()

			cloud_hash = sha256(blob.download_as_string()).hexdigest()

			if local_hash != cloud_hash:
				blob.delete()
				raise Exception("Hashes do not match. Corrupted File.")

			logger.info(f"Store,Upload,Success,{storage_attempts},,")

			break

		except Exception as e:

			logger.warning(f"Store,Upload,Failure,{storage_attempts},{e},")
			storage_attempts += 1

	if storage_attempts >= max_tries:
		raise Exception("Too Many Storage Attempts.")

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
	project_name = METRIC_CLIENT.project_path(CONFIG['gcp_project_id'])
	METRIC_CLIENT.create_time_series(project_name, [series])

def delete_gcp_metric():

	project_id = "XXX"
	metric_name = "XXX"
	name = f"projects/{project_id}/metricDescriptors/custom.googleapis.com/{metric_name}"	

	METRIC_CLIENT.delete_metric_descriptor(name)
	print('Deleted metric descriptor {}.'.format(name))

def create_gcp_metric(metric_name, value_type):

	with open("../config.json", "r") as file:
		CONFIG = json.loads(file.read())

	project_name = METRIC_CLIENT.project_path(CONFIG['gcp_project_id'])

	descriptor = monitoring_v3.types.MetricDescriptor()
	descriptor.type = f'custom.googleapis.com/{metric_name}'

	descriptor.metric_kind = (monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
	descriptor.value_type = (monitoring_v3.enums.MetricDescriptor.ValueType[value_type])

	descriptor.description = 'This is a simple example of a custom metric.'
	descriptor = METRIC_CLIENT.create_metric_descriptor(project_name, descriptor)

	print('Created {}.'.format(descriptor.name))

if __name__ == '__main__':

	# # OscraP
	# create_gcp_metric("oscrap_options_sucess", "DOUBLE")
	# create_gcp_metric("oscrap_key_stats_sucess", "DOUBLE")
	# create_gcp_metric("oscrap_analysis_sucess", "DOUBLE")
	# create_gcp_metric("oscrap_ohlc_sucess", "DOUBLE")
	
	# # Rates
	# create_gcp_metric("rates_success_indicator", "INT64")

	# # RSS
	# create_gcp_metric("rss_daily_item_total", "INT64")
	# create_gcp_metric("rss_daily_item_counter", "INT64")
	# create_gcp_metric("rss_daily_item_uniques", "INT64")
