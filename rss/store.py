from datetime import datetime, timedelta
from google.cloud import storage
from const import DIR, logger
from hashlib import md5
import tarfile as tar
import pandas as pd
import sys, os
import json

def compress_files():

	back_file_name = datetime.now() - timedelta(days = 1)
	back_file_name = back_file_name.strftime('%Y-%m-%d')
	tar_file_name = back_file_name + ".tar.xz"

	back_file_name = f'{DIR}/news_data_backup/{back_file_name}.txt'
	tar_file_name = f'{DIR}/news_data_backup/{tar_file_name}'

	files = os.listdir(f"{DIR}/news_data")
	files.remove('.gitignore')
	
	data = []
	for file in files:
		with open(f'{DIR}/news_data/{file}', 'r') as data_file:
			data.extend(json.loads(data_file.read()))
	logger.info(f"RSS,Storage,Data,{len(data)}")

	hashes = []
	for item in data:
		hashes.append(md5(json.dumps(item).encode()).hexdigest())
	
	hashes = list(set(hashes))
	data = [data[hashes.index(hash_)] for hash_ in hashes]
	logger.info(f"RSS,Storage,Unique Data,{len(data)}")

	with open(back_file_name, "w") as file:
		file.write(json.dumps(data))

	with tar.open(tar_file_name, mode="x:xz") as tar_file:
		tar_file.add(back_file_name, arcname=os.path.basename(back_file_name))

	file_size = os.stat(tar_file_name).st_size / 1_000_000
	if file_size > 0:
		for file in files:
			os.remove(f'{DIR}/news_data/{file}')
	else:
		raise Exception("TarFile Corrupted. File Size 0.")

	return tar_file_name

def send_to_bucket(tar_file_name):

	storage_client = storage.Client()
	bucket = storage_client.bucket("oscrap_storage")

	destination_name = os.path.basename(tar_file_name)
	blob = bucket.blob(f"rss/{destination_name}")
	blob.upload_from_filename(tar_file_name)
	logger.info(f"RSS,Storage,Upload Name,rss/{destination_name}")

if __name__ == '__main__':

	try:

		tar_file_name = compress_files()
		send_to_bucket(tar_file_name)
		logger.info(f"RSS,Storage,Success,")

	except Exception as e:

		logger.warning(f"RSS,Storage,Failure,{e}")
