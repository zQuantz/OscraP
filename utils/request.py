import requests

headers_mobile = {'User-Agent' : """Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46
									(KHTML, like Gecko) Version/9.0 Mobile/13B137 Safari/601.1"""}

def request(CONFIG, url):

	max_tries = 5
	request_attempts = 0

	while request_attempts < max_tries:

		try:
			return requests.get(url, headers = headers_mobile, timeout = CONFIG['timeout'])
		except Exception as e:
			self.logger.warning(f"{self.ticker},{self.batch_id},Request Error,{request_attempts},{e}")
		request_attempts += 1

	if request_attempts >=  max_tries:
		raise Exception("Max Request Attempts Reached.")
