from const import DIR, CONFIG, logger

import sqlalchemy as sql
import tarfile as tar
import pandas as pd
import numpy as np
import sys, os
import json

sys.path.append(f"{DIR}/../utils")
from send_gcp_metric import send_gcp_metric
from send_to_gcp import send_to_gcp

###################################################################################################

attrs = {"class" : "t-chart"}
URL = "https://www.treasury.gov/resource-center/data-chart-center/interest-rates/pages/textview.aspx?data=yield"

BUCKET_PREFIX = CONFIG['gcp_bucket_prefix']
BUCKET_NAME = CONFIG['gcp_bucket_name']
DATE = CONFIG['date']

###################################################################################################

def store():

	with tar.open(f"{DIR}/rate_data/{DATE}.tar.xz", "x:xz") as tar_file:

		filename = f"{DIR}/rate_data/{DATE}.csv"
		tar_file.add(filename, os.path.basename(filename))

	send_to_gcp(BUCKET_PREFIX, BUCKET_NAME, f"{DATE}.tar.xz", f"{DIR}/rate_data/", logger=logger)
	os.remove(filename)

def collect():

	logger.info(f"Downloading Table: {URL}")
	df = pd.read_html(URL, attrs=attrs)
	logger.info(f"Number of tables found: {len(df)}")

	if len(df) != 1:
		return

	df = df[0]
	df.columns = [
		"date_current",
		"_1_month",
		"_2_months",
		"_3_months",
		"_6_months",
		"_1_year",
		"_2_years",
		"_3_years",
		"_5_years",
		"_7_years",
		"_10_years",
		"_20_years",
		"_30_years",
	]

	df['date_current'] = pd.to_datetime(df.date_current)
	df = df.sort_values('date_current', ascending=False)
	df = df.reset_index(drop=True)

	###############################################################################################

	df = df[df.date_current == DATE]
	logger.info(f"Number of items after filter: {len(df)}")

	if len(df) == 0:
		raise Exception("Data not up to date.")

	engine = sql.create_engine(CONFIG['db_address'])

	df.to_sql("rates", engine, if_exists='append', index=False, chunksize=10_000)
	df.to_csv(f"{DIR}/rate_data/{DATE}.csv")

if __name__ == '__main__':

	try:
		
		collect()
		store()

		metric = 1

	except Exception as e:

		logger.info(e)
		metric = 0

	send_gcp_metric(CONFIG, "rates_success_indicator", "int64_value", metric)
