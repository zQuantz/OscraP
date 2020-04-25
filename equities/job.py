from unit_tests import check_number_of_options, check_null_percentage, check_ohlc
from const import DIR, date_today, logger
from alert import send_scraping_report
from index import send_to_database
from datetime import datetime

from ticker import Ticker
import sqlalchemy as sql
import pandas as pd
import numpy as np
import sys, os
import shutil
import time

###################################################################################################

engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_finance")
query = f"""
    SELECT
        *
    FROM
        instruments
    WHERE
        market_cap >= {1_000_000}
    ORDER BY market_cap DESC
"""

conn = engine.connect()
df = pd.read_sql(query, conn)
conn.close()

usd = df[~df.exchange_code.isin(["TSX"])].iloc[:900, :]
cad = df[df.exchange_code.isin(["TSX"])].iloc[:100, :]
tickers = (usd.ticker.values.tolist() + cad.ticker.values.tolist())

df = df[df.ticker.isin(tickers)]
df = df[['ticker', 'name']]
ticker_dict = df.set_index("ticker").to_dict()['name']

###################################################################################################

def collect_data():

	for i, ticker in enumerate(ticker_dict):
		
		try:

			Ticker(ticker, logger, date_today) ; time.sleep(5)
			logger.info(f"{ticker},Ticker,Success,")

		except Exception as e:

			logger.warning(f"{ticker},Ticker,Failure,{e}")

		pct = (i + 1) / len(ticker_dict)
		pct = np.round(100 * pct, 4) 
		logger.info(f"SCRAPER,PROGRESS,{pct}%,")

def collect_data_again(faults):

	for i, ticker in enumerate(faults):
		
		try:

			retries = {
				key : key in faults[ticker]
				for key in ['analysis', 'key_stats', 'ohlc', 'options']
			}

			ticker_obj = Ticker(ticker, logger, date_today, retries, faults[ticker])
			faults[ticker] = ticker_obj.fault_dict
			time.sleep(5)

			logger.info(f"{ticker},Re-Ticker,Success,")

		except Exception as e:

			logger.warning(f"{ticker},Re-Ticker,Failure,{e}")

		pct = (i + 1) / len(faults)
		pct = np.round(100 * pct, 4) 
		logger.info(f"SCRAPER,RE-PROGRESS,{pct}%,")

	return faults

def database_and_alerts():

	max_tries = 5
	indexing_attempts = 0

	while indexing_attempts < max_tries:
		
		try:
			db_stats = send_to_database()
			db_flag = 1
			break

		except Exception as e:

			logger.warning(e)
			db_stats = [(0,0), (0,0), (0,0), (0,0)]
			db_flag = 0

		indexing_attempts += 1

	return db_flag, db_stats, indexing_attempts

def log_scraper_health():

	collected_options = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{date_today}/options')]
	collected_ohlc = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{date_today}/ohlc')]
	collected_analysis = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{date_today}/analysis')]
	collected_key_stats = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{date_today}/key_stats')]

	success = {
		"options" : 0,
		"ohlc" : 0,
		"key_stats" : 0,
		"analysis" : 0
	}

	failure = {
		"options" : 0,
		"ohlc" : 0,
		"key_stats" : 0,
		"analysis" : 0
	}

	for ticker in ticker_dict:

		if ticker in collected_options:
			success['options'] += 1
		else:
			failure['options'] += 1

		if ticker in collected_ohlc:
			success['ohlc'] += 1
		else:
			failure['ohlc'] += 1

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
	os.mkdir(f'{DIR}/financial_data/{date_today}/ohlc')
	os.mkdir(f'{DIR}/financial_data/{date_today}/key_stats')
	os.mkdir(f'{DIR}/financial_data/{date_today}/analysis')

def fix_faults():

	def add_to_faults(key, obj, faults):
	    for ticker in obj:
	        try:
	            faults[ticker][key] = obj[ticker]
	        except Exception as e:
	            faults[ticker] = {
	                key : obj[ticker]
	            }
	    return faults

	analysis_faults = check_null_percentage("analysis")
	key_stats_faults = check_null_percentage("key_stats")
	options_faults = check_number_of_options()
	ohlc_faults = check_ohlc()

	faults = add_to_faults("analysis", analysis_faults, {})
	faults = add_to_faults("key_stats", key_stats_faults, faults)
	faults = add_to_faults("options", options_faults, faults)
	faults = add_to_faults("ohlc", ohlc_faults, faults)

	faults = collect_data_again(faults)

	faults_summary = {
	    key : {}
	    for key in ["analysis", "key_stats", "ohlc", "options"]
	}
	for ticker in faults:
	    for key in faults[ticker]:
	        faults_summary[key][ticker] = faults[ticker][key]

	return faults_summary

def main():

	collect_data()
	faults_summary = fix_faults()

	success, failure = log_scraper_health()
	db_flag, db_stats, indexing_attempts = database_and_alerts()
	send_scraping_report(success, failure, faults_summary, db_flag, db_stats, indexing_attempts)

if __name__ == '__main__':

	logger.info(f"SCRAPER,JOB,INITIATED,{date_today}")

	init_folders()
	main()

	logger.info(f"SCRAPER,JOB,TERMINATED,{date_today}")
