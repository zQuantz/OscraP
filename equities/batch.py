from unit_tests import check_number_of_options, check_null_percentage, check_ohlc
from const import DIR, logger
from ticker import Ticker
from index import index
import numpy as np
import sys, os
import time

with open(f"{DIR}/static/date.txt", "w") as file:
	DATE = file.read()

def collect_data(batch_id, tickers):

	for i, ticker in enumerate(tickers):
		
		try:

			Ticker(ticker, logger, DATE, batch_id) ; time.sleep(5)
			logger.info(f"{ticker},{batch_id},Ticker,Success,")

		except Exception as e:

			logger.warning(f"{ticker},{batch_id},Ticker,Failure,{e}")

		pct = (i + 1) / len(tickers)
		pct = np.round(100 * pct, 4) 
		logger.info(f"SCRAPER,{batch_id},PROGRESS,{pct}%,")

def collect_data_again(batch_id, faults):

	for i, ticker in enumerate(faults):

		try:

			retries = {
				key : key in faults[ticker]
				for key in ['analysis', 'key_stats', 'ohlc', 'options']
			}

			ticker_obj = Ticker(ticker, logger, DATE, batch_id, retries, faults[ticker])
			faults[ticker] = ticker_obj.fault_dict
			time.sleep(5)

			logger.info(f"{ticker},{batch_id},Re-Ticker,Success,")

		except Exception as e:

			logger.warning(f"{ticker},{batch_id},Re-Ticker,Failure,{e}")

		pct = (i + 1) / len(faults)
		pct = np.round(100 * pct, 4) 
		logger.info(f"SCRAPER,{batch_id},RE-PROGRESS,{pct}%,")

	return faults

def index_data(batch_id):

	max_tries = 5
	indexing_attempts = 0

	while indexing_attempts < max_tries:
		
		try:
			
			db_stats = index()
			db_flag = 1
			
			logger.info(f"SCRAPER,{batch_id},INDEXING,SUCCESS,{indexing_attempts}")
			
			break

		except Exception as e:

			logger.warning(f"SCRAPER,{batch_id},INDEXING,FAILURE,{e}")
			
			db_stats = [(0,0), (0,0), (0,0), (0,0)]
			db_flag = 0

		indexing_attempts += 1

	return db_flag, db_stats, indexing_attempts

def fix_faults(batch_id, tickers):

	def add_to_faults(key, obj, faults):
	    for ticker in obj:
	        try:
	            faults[ticker][key] = obj[ticker]
	        except Exception as e:
	            faults[ticker] = {
	                key : obj[ticker]
	            }
	    return faults

	max_tries = 5
	query_attempts = 0

	while query_attempts < max_tries:

		try:

			analysis_faults = check_null_percentage(tickers, "analysis")
			key_stats_faults = check_null_percentage(tickers, "key_stats")
			options_faults = check_number_of_options(tickers)
			ohlc_faults = check_ohlc(tickers)

			logger.info(f"SCRAPER,{batch_id},FAULTS,SUCCESS,{query_attempts}")

			break

		except Exception as e:

			logger.info(f"SCRAPER,{batch_id},FAULTS,FAILURE,{e}")

		query_attempts += 1

	faults = add_to_faults("analysis", analysis_faults, {})
	faults = add_to_faults("key_stats", key_stats_faults, faults)
	faults = add_to_faults("options", options_faults, faults)
	faults = add_to_faults("ohlc", ohlc_faults, faults)

	faults = collect_data_again(batch_id, faults)

	faults_summary = {
	    key : {}
	    for key in ["analysis", "key_stats", "ohlc", "options"]
	}
	for ticker in faults:
	    for key in faults[ticker]:
	        faults_summary[key][ticker] = faults[ticker][key]

	return faults_summary

def main(batch_id, tickers):

	logger.info(f"SCRAPER,{batch_id},INITIATED,,")

	collect_data(batch_id, tickers)
	faults_summary = fix_faults(batch_id, tickers)

	db_flag, db_stats, indexing_attempts = index_data(batch_id)

	logger.info(f"SCRAPER,{batch_id},TERMINATED,,")

	return faults_summary, db_flag, db_stats, indexing_attempts
