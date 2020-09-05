from const import DIR, DATA, DATE, CONFIG, logger, t_map, _connector

import tarfile as tar
import pandas as pd
import numpy as np
import sys, os
import json

sys.path.append(f"{DIR}/../utils")
from gcp import send_to_bucket, send_gcp_metric
from send_email import send_email

###################################################################################################

attrs = {"class" : "t-chart"}
URL = "https://www.treasury.gov/resource-center/data-chart-center/interest-rates/pages/textview.aspx?data=yield"

BUCKET_PREFIX = CONFIG['gcp_bucket_prefix']
BUCKET_NAME = CONFIG['gcp_bucket_name']

###################################################################################################

def store():

	with tar.open(f"{DATA}.tar.xz", "x:xz") as tar_file:

		filename = f"{DATA}.csv"
		tar_file.add(filename, os.path.basename(filename))

	send_to_bucket(BUCKET_PREFIX,
				   BUCKET_NAME,
				   f"{DATE}.tar.xz",
				   f"{DIR}/rate_data",
				   logger=logger)

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

	_connector.write("treasuryratesBACK", df)
	df.to_csv(f"{DATA}.csv", index=False)

	###############################################################################################

	r_map = df.iloc[-1, 1:].values
	r_map = np.array([0] + r_map.tolist())

	def get_rate(t):
		
		if t >= 30 * 360:
			return r_map[-1]

		b1 = t_map <= t
		b2 = t_map > t

		r1 = r_map[b1][-1]
		r2 = r_map[b2][0]

		t1 = t_map[b1][-1]
		t2 = t_map[b2][0]

		interpolated_rate = (t - t1) / (t2 - t1)
		interpolated_rate *= (r2 - r1)

		return interpolated_rate + r1

	rm_df = pd.DataFrame()
	rm_df['days_to_expiry'] = np.arange(0, 365 * 10 + 1).astype(int)
	rm_df['rate'] = rm_df.days_to_expiry.apply(get_rate)
	rm_df['date_current'] = DATE

	_connector.write("treasuryratemapBACK", rm_df)

	return df

if __name__ == '__main__':

	try:

		df = collect()
		store()
		send_email(CONFIG, "Interest Rate Summary", df.to_html(), [], logger)
		metric = 1

	except Exception as e:

		logger.warning(e)
		body = f"<p>Process Failed. {e}</p>"
		send_email(CONFIG, "Interest Rate Summary - FAILED", body, [], logger)
		metric = 0

	send_gcp_metric(CONFIG, "rates_success_indicator", "int64_value", metric)
