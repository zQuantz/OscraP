from google.cloud import monitoring_v3

if __name__ == '__main__':

	client = monitoring_v3.MetricServiceClient()

	project_id = "XXX"
	metric_name = "XXX"
	name = f"projects/{project_id}/metricDescriptors/custom.googleapis.com/{metric_name}"	

	client.delete_metric_descriptor(name)
	print('Deleted metric descriptor {}.'.format(name))