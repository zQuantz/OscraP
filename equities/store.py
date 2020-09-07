from const import DIR, DATE, DATA, CONFIG, logger

from google.cloud import storage
import tarfile as tar
import pandas as pd
import sys, os
import shutil

sys.path.append(f"{DIR}/../utils")
from gcp import send_to_bucket

###################################################################################################

BUCKET_PREFIX = CONFIG['gcp_bucket_prefix']
BUCKET_NAME = CONFIG['gcp_bucket_name']

###################################################################################################

def aggregate():

	data = {}
	for folder in DATA.iterdir():

		data[folder.name] = []

		for file in folder.iterdir():
			data[folder.name].append(pd.read_csv(file))

		if len(data[folder.name]) == 0:
			continue

		df = pd.concat(data[folder.name])
		df.to_csv(f"{DATA}/{folder.name}.csv", index=False)

def compress():

	with tar.open(f"{DATA}.tar.xz", "x:xz") as tar_file:

		for file in DATA.iterdir():

			if '.csv' not in file.name:
				continue

			tar_file.add(file, file.name)

def remove():

	for folder in (DIR / "financial_data").iterdir():
		if folder.is_dir():
			shutil.rmtree(folder)

def main():

	logger.info(f"SCRAPER,STORE,INITIATED,,")
	
	try:
	
		aggregate() ; compress()

		send_to_bucket(BUCKET_PREFIX, BUCKET_NAME, f"{DATE}.tar.xz",
				    f"{DIR}/financial_data", logger=logger)
		
		remove()
		
		logger.info(f"SCRAPER,STORE,SUCCESS,,")

	except Exception as e:

		logger.warning(f"SCRAPER,STORE,FAILURE,{e},")

	logger.info(f"SCRAPER,STORE,TERMINATED,,")

if __name__ == '__main__':

	main()
