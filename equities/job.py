from send import send_scraping_report, send_to_database
from unit_tests import check_number_of_options
from const import DIR, date_today, logger
from datetime import datetime

from ticker import Ticker
import pandas as pd
import sys, os
import pickle
import shutil
import time

with open(f'{DIR}/static/tickers.pickle', 'rb') as file:
	ticker_dict = pickle.load(file)

def collect_data():

	# ticker_dict = {
	# 	"TSLA" : "None",
	# 	"DOL.TO" : "None",
	# 	"BA" : "None",
	# 	"SPLK" : "None",
	# 	"SQ" : "None"
	# }

	for ticker in ticker_dict:
		
		try:

			Ticker(ticker, logger) ; time.sleep(5)
			logger.info(f"{ticker},Ticker,Success,")

		except Exception as e:

			logger.warning(f"{ticker},Ticker,Failure,{e}")

def collect_data_again(unhealthy_tickers):

	for ticker in unhealthy_tickers:
		
		try:

			ticker = Ticker(ticker, logger) ; time.sleep(5)
			unhealthy_tickers[ticker]['new_options'] = len(ticker.options)
			logger.info(f"{ticker},Re-Ticker,Success,")

		except Exception as e:

			unhealthy_tickers[ticker]['new_options'] = -1
			logger.warning(f"{ticker},Re-Ticker,Failure,{e}")

	return unhealthy_tickers

def database_and_alerts():

	max_tries = 5
	indexing_faults = 0

	while indexing_faults < max_tries:
		
		try:

			db_stats = send_to_database()
			db_flag = 1
			break

		except Exception as e:

			logger.warning(e)
			db_stats = (0, 0, pd.DataFrame({"None" : [True]}))
			db_flag = 0

		indexing_faults += 1

	return db_flag, db_stats, indexing_faults

def log_scraper_health():

	collected_options = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{date_today}/options')]
	collected_equities = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{date_today}/equities')]
	collected_analysis = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{date_today}/analysis')]
	collected_key_stats = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{date_today}/key_stats')]

	success = {
		"options" : 0,
		"equities" : 0,
		"key_stats" : 0,
		"analysis" : 0
	}

	failure = {
		"options" : 0,
		"equities" : 0,
		"key_stats" : 0,
		"analysis" : 0
	}

	for ticker in ticker_dict:

		if ticker in collected_options:
			success['options'] += 1
		else:
			failure['options'] += 1

		if ticker in collected_equities:
			success['equities'] += 1
		else:
			failure['equities'] += 1

		if ticker in collected_analysis:
			success['analysis'] += 1
		else:
			failure['analysis'] += 1

		if ticker in collected_key_stats:
			success['key_stats'] += 1
		else:
			failure['key_stats'] += 1

	shutil.make_archive(f"{DIR}/financial_data/{date_today}", "zip", f"{DIR}/financial_data/{date_today}")

	return success, failure

def init_folders():

	os.mkdir(f'{DIR}/financial_data/{date_today}')
	os.mkdir(f'{DIR}/financial_data/{date_today}/options')
	os.mkdir(f'{DIR}/financial_data/{date_today}/equities')
	os.mkdir(f'{DIR}/financial_data/{date_today}/key_stats')
	os.mkdir(f'{DIR}/financial_data/{date_today}/analysis')

def main():

	collect_data()

	unhealthy_tickers = check_number_of_options()
	if len(unhealthy_tickers) > 0:
		unhealthy_tickers = collect_data_again(unhealthy_tickers)

	success, failure = log_scraper_health()
	db_flag, db_stats, indexing_faults = database_and_alerts()
	send_scraping_report(success, failure, unhealthy_tickers, db_flag, db_stats, indexing_faults)

if __name__ == '__main__':


	logger.info(f"SCRAPER,JOB,INITIATED,{date_today}")

	init_folders()
	main()

	logger.info(f"SCRAPER,JOB,TERMINATED,{date_today}")
