from datetime import datetime, timedelta
from const import DIR, CONFIG, logger
from hashlib import md5
import tarfile as tar
import pandas as pd
import sys, os
import json

sys.path.append(f"{DIR}/../utils")
from send_gcp_metric import send_gcp_metric
from send_to_gcp import send_to_gcp

def compress_files():

	back_file_name = datetime.now() - timedelta(days = 1)
	back_file_name = back_file_name.strftime('%Y-%m-%d')
	tar_file_name = back_file_name + ".tar.xz"

	back_file_name = f'{DIR}/news_data_backup/{back_file_name}.txt'
	tar_file_name = f'{DIR}/news_data_backup/{tar_file_name}'

	files = os.listdir(f"{DIR}/news_data")
	files = [f"{DIR}/news_data/{file}" for file in files]

	files = sorted(files, key=os.path.getmtime)
	files = files[::-1]
	
	files.remove(f"{DIR}/news_data/.gitignore")

	ctr = 0
	data, hashes = list(), set()
	for file in files:

		with open(file, "r") as data_file:

			items = json.loads(data_file.read())

			for item in items:

				ctr += 1

				item_ = item.copy()
				item_.pop("oscrap_acquisition_datetime")

				hash_ = md5(json.dumps(item_).encode()).hexdigest()

				if hash_ in hashes:
					continue

				data.append(item)
				hashes.add(hash_)

	logger.info(f"RSS,Storage,Data,{ctr}")
	logger.info(f"RSS,Storage,Unique Data,{len(hashes)}")
	
	send_gcp_metric(CONFIG, "rss_daily_item_uniques", "int64_value", len(hashes))
	send_gcp_metric(CONFIG, "rss_daily_item_total", "int64_value", ctr)

	with open(back_file_name, "w") as file:
		file.write(json.dumps(data))

	with tar.open(tar_file_name, mode="x:xz") as tar_file:
		tar_file.add(back_file_name, arcname=os.path.basename(back_file_name))

	file_size = os.stat(tar_file_name).st_size / 1_000_000
	if file_size > 0:
		for file in files:
			os.remove(file)
	else:
		raise Exception("TarFile Corrupted. File Size 0.")

	return tar_file_name

if __name__ == '__main__':

	try:

		tar_file_name = compress_files()

		send_to_gcp(
			CONFIG['gcp_bucket_prefix'],
			CONFIG['gcp_bucket_name'],
			os.path.basename(tar_file_name),
			os.path.dirname(tar_file_name),
			logger=logger
		)

		logger.info(f"RSS,Storage,Success,")

	except Exception as e:

		logger.warning(f"RSS,Storage,Failure,{e}")
