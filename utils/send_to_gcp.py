from dummy_logger import DummyLogger
from google.cloud import storage
from hashlib import sha256

def send_to_gcp(bucket_prefix, bucket_name, filename, filepath, logger=None):

	if not logger:
		logger = DummyLogger()

	max_tries = 5
	storage_attempts = 0

	fullname = f"{filepath}/{filename}"

	while storage_attempts < max_tries:

		try:

			storage_client = storage.Client()
			bucket = storage_client.bucket(bucket_name)

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