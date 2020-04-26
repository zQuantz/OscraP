from const import date_today, DIR, logger
from google.cloud import storage
from hashlib import sha256
import tarfile as tar
import pandas as pd
import numpy as np
import sys, os
import shutil

def aggregate():

	data = {}
	for folder in os.listdir(f"{DIR}/financial_data/{date_today}"):

		data[folder] = []

		for file in os.listdir(f"{DIR}/financial_data/{date_today}/{folder}"):

			df = pd.read_csv(f"{DIR}/financial_data/{date_today}/{folder}/{file}")
			df['date_current'] = date_today
			df['ticker'] = file.split('_')[0]

			if folder == "options":
				df['option_id'] = (df.ticker + ' ' + df.expiration_date + ' ' + df.option_type
							  	   + np.round(df.strike_price, 2).astype(str))

			data[folder].append(df)

		df = pd.concat(data[folder]).reset_index(drop=True)

		first_cols = ["ticker", "date_current"]
		if folder == "options":
			first_cols += ["option_id"]
		next_cols = [col for col in df.columns if col not in first_cols]

		df = df[first_cols + next_cols]
		data[folder] = df

		data[folder].to_csv(f"{DIR}/financial_data/{date_today}/{folder}.csv", index=False)

def compress():

	with tar.open(f"{DIR}/financial_data/{date_today}.tar.xz", "x:xz") as tar_file:

		for file in os.listdir(f"{DIR}/financial_data/{date_today}"):

			if '.csv' not in file:
				continue

			filename = f"{DIR}/financial_data/{date_today}/{file}"
			tar_file.add(filename, os.path.basename(filename))

def send_and_verify():

	max_tries = 5
	storage_attempts = 0

	while storage_attempts < max_tries:

		try:

			storage_client = storage.Client()
			bucket = storage_client.bucket("oscrap_storage")

			destination_name = f"equities/{date_today}.tar.xz"
			blob = bucket.blob(destination_name)
			blob.upload_from_filename(f"{DIR}/financial_data/{destination_name}")
			
			with open(f"{DIR}/financial_data/{destination_name}", "rb") as file:
				local_hash = sha256(file.read()).hexdigest()

			cloud_hash = sha256(blob.download_as_string()).hexdigest()

			if local_hash != cloud_hash:
				raise Exception("Hashes do not match. Corrupted File.")

			logger.info(f"Store,Upload,Success,{storage_attempts},,")

			break

		except Exception as e:

			logger.warning(f"Store,Upload,Failure,{storage_attempts},{e},")
			storage_attempts += 1

def remove():

	for folder in os.listdir(f"{DIR}/financial_data/{date_today}"):
		folder = f"{DIR}/financial_data/{date_today}/{folder}"
		if os.path.isdir(folder):
			shutil.rmtree(folder)

	os.remove(f"{DIR}/financial_data/{date_today}.zip")

def main():

	logger.info(f"SCRAPER,STORE,INITIATED,,")
	
	try:
	
		aggregate() ; compress()
		send_and_verify() ; remove()
		
		logger.info(f"SCRAPER,STORE,SUCCESS,,")

	except Exception as e:

		logger.warning(f"SCRAPER,STORE,FAILURE,{e},")

	logger.info(f"SCRAPER,STORE,TERMINATED,,")

if __name__ == '__main__':

	main()