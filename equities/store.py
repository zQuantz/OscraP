from const import DIR, CONFIG, logger

from google.cloud import storage
from hashlib import sha256
import tarfile as tar
import pandas as pd
import sys, os
import shutil

sys.path.append(f"{DIR}/../utils")
from send_to_gcp import send_to_gcp

###################################################################################################

BUCKET_PREFIX = CONFIG['gcp_bucket_prefix']
BUCKET_NAME = CONFIG['gcp_bucket_name']
DATE = CONFIG['date']

###################################################################################################

def aggregate():

	data = {}
	for folder in os.listdir(f"{DIR}/financial_data/{DATE}"):

		data[folder] = []

		for file in os.listdir(f"{DIR}/financial_data/{DATE}/{folder}"):

			df = pd.read_csv(f"{DIR}/financial_data/{DATE}/{folder}/{file}")
			df['date_current'] = DATE
			df['ticker'] = file.split('_')[0]

			if folder == "options":
				df['option_id'] = (df.ticker + ' ' + df.expiration_date + ' ' + df.option_type
							  	   + df.strike_price.round(2).astype(str))

			data[folder].append(df)

		if len(data[folder]) == 0:
			continue

		df = pd.concat(data[folder]).reset_index(drop=True)

		first_cols = ["ticker", "date_current"]
		if folder == "options":
			first_cols += ["option_id"]
		next_cols = [col for col in df.columns if col not in first_cols]

		df = df[first_cols + next_cols]
		data[folder] = df

		data[folder].to_csv(f"{DIR}/financial_data/{DATE}/{folder}.csv", index=False)

def compress():

	with tar.open(f"{DIR}/financial_data/{DATE}.tar.xz", "x:xz") as tar_file:

		for file in os.listdir(f"{DIR}/financial_data/{DATE}"):

			if '.csv' not in file:
				continue

			filename = f"{DIR}/financial_data/{DATE}/{file}"
			tar_file.add(filename, os.path.basename(filename))

def remove():

	for folder in os.listdir(f"{DIR}/financial_data/"):
		folder = f"{DIR}/financial_data/{folder}"
		if os.path.isdir(folder):
			shutil.rmtree(folder)

def main():

	logger.info(f"SCRAPER,STORE,INITIATED,,")
	
	try:
	
		aggregate() ; compress()

		send_to_gcp(BUCKET_PREFIX, BUCKET_NAME, f"{DATE}.tar.xz",
				    f"{DIR}/financial_data/", logger=logger)
		
		remove()
		
		logger.info(f"SCRAPER,STORE,SUCCESS,,")

	except Exception as e:

		logger.warning(f"SCRAPER,STORE,FAILURE,{e},")

	logger.info(f"SCRAPER,STORE,TERMINATED,,")

if __name__ == '__main__':

	main()
