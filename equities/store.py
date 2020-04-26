from const import date_today, DIR
from google.cloud import storage
import tarfile as tar
import pandas as pd
import numpy as np
import sys, os
import shutil

storage_client = storage.Client()
bucket = storage_client.bucket("oscrap_storage")
date_today = "2020-04-25"

def aggregate_files():

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

def compress_files():

	with tar.open(f"{DIR}/financial_data/{date_today}.tar.xz", "x:xz") as tar_file:

		for file in os.listdir(f"{DIR}/financial_data/{date_today}"):

			if '.csv' not in file:
				continue

			filename = f"{DIR}/financial_data/{date_today}/{file}"
			tar_file.add(filename, os.path.basename(filename))

def remove_folders():

	for folder in os.listdir(f"{DIR}/financial_data/{date_today}"):
		folder = f"{DIR}/financial_data/{date_today}/{folder}"
		if os.path.isdir(folder):
			shutil.rmtree(folder)

if __name__ == '__main__':

	aggregate_files()
	compress_files()
	remove_folders()
