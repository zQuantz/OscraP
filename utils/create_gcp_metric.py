from google.cloud import monitoring_v3

PROJECT_ID = "dev-utility-270718"
CLIENT = monitoring_v3.MetricServiceClient()
PROJECT_NAME = CLIENT.project_path("dev-utility-270718")

def create_gcp_metric(metric_name):

	client = monitoring_v3.MetricServiceClient()
	project_name = client.project_path(PROJECT_ID)

	descriptor = monitoring_v3.types.MetricDescriptor()
	descriptor.type = f'custom.googleapis.com/{metric_name}'

	descriptor.metric_kind = (monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
	descriptor.value_type = (monitoring_v3.enums.MetricDescriptor.ValueType.DOUBLE)

	descriptor.description = 'This is a simple example of a custom metric.'
	descriptor = client.create_metric_descriptor(project_name, descriptor)

	print('Created {}.'.format(descriptor.name))

if __name__ == '__main__':

	# create_gcp_metric("oscrap_options_sucess")
	# create_gcp_metric("oscrap_key_stats_sucess")
	# create_gcp_metric("oscrap_analysis_sucess")
	# create_gcp_metric("oscrap_ohlc_sucess")
