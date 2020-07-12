from google.cloud import monitoring_v3
import json

def create_gcp_metric(metric_name, value_type):

	with open("../config.json", "r") as file:
		CONFIG = json.loads(file.read())

	client = monitoring_v3.MetricServiceClient()
	project_name = client.project_path(CONFIG['gcp_project_id'])

	descriptor = monitoring_v3.types.MetricDescriptor()
	descriptor.type = f'custom.googleapis.com/{metric_name}'

	descriptor.metric_kind = (monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
	descriptor.value_type = (monitoring_v3.enums.MetricDescriptor.ValueType[value_type])

	descriptor.description = 'This is a simple example of a custom metric.'
	descriptor = client.create_metric_descriptor(project_name, descriptor)

	print('Created {}.'.format(descriptor.name))

if __name__ == '__main__':

	create_gcp_metric("test_metric_test", "DOUBLE")


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

