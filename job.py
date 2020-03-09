from send import send_scraping_report, send_to_database
from const import DIR, date_today, logger
from datetime import datetime

from ticker import Ticker
import pandas as pd
import sys, os
import pickle
import shutil
import time

with open(f'{DIR}/data/tickers.pickle', 'rb') as file:
	ticker_dict = pickle.load(file)

def collect_data():

	for ticker in ticker_dict:
		
		try:
			
			logger.info(f"Processing: {ticker}")
			
			Ticker(ticker, logger)
			
			time.sleep(5)
			logger.info(f"{ticker} was collected successfully.")
		
		except Exception as e:
			
			logger.info(f"{ticker} was not collected successfully.")
			logger.warning(e)

def database_and_alerts():

	max_tries = 5
	ctr = 0

	while ctr < max_tries:
		
		try:
			db_stats = send_to_database()
			db_flag = 1
			break
		except Exception as e:
			logger.warning(e)
			db_stats = (0, 0, pd.DataFrame({"None" : [True]}))
			db_flag = 0

		ctr += 1

	return db_flag, db_stats, ctr

def log_scraper_health():

	collected_tickers = [file.split('_')[0] for file in os.listdir(f'{DIR}/options_data/{date_today}')]

	success = []
	failure = []

	for ticker in ticker_dict:
		if ticker in collected_tickers:
			success.append(ticker)
		else:
			failure.append(ticker)

	with open(f'{DIR}/options_data/{date_today}/successful_tickers.txt', 'w') as file:
		
		file.write(f"Ticker\tCompany Name\tFile Size\n")
		for ticker in success:

			size = os.stat(f'{DIR}/options_data/{date_today}/{ticker}_{date_today}.csv').st_size / 1000
			size = "%.2f kb" % size
			file.write(f"{ticker}\t{ticker_dict[ticker]}\t{size}\n")

	with open(f'{DIR}/options_data/{date_today}/failed_tickers.txt', 'w') as file:
		
		file.write(f"Ticker\tCompany Name\n")
		for ticker in failure:
			file.write(f"{ticker}\t{ticker_dict[ticker]}\n")

	shutil.make_archive(f"{DIR}/options_data/{date_today}", "zip", f"{DIR}/options_data/{date_today}")

	return success, failure

def main():

	collect_data()
	success, failure = log_scraper_health()
	db_flag, db_stats, ctr = database_and_alerts()

	send_scraping_report(success, failure, db_flag, db_stats, ctr)

if __name__ == '__main__':

	logger.info(f"OPTION SCRAPER - {date_today}")
	logger.info(f"Job initiated.")
	os.mkdir(f'{DIR}/options_data/{date_today}')

	main()

	logger.info("Job terminated.")
