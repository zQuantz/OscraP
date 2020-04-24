from unit_tests import check_number_of_options, check_null_percentage
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

		logger.info(f"SCRAPER,PROGRESS,{np.round( 100 * ((i + 1) / len(ticker_dict)), 4)}%,")

def collect_data_again(unhealthy_tickers):

	for i, ticker in enumerate(unhealthy_tickers):
		
		try:

			ticker_obj = Ticker(ticker, logger, date_today, isRetry = True) ; time.sleep(5)

			old = pd.read_csv(f'{DIR}/financial_data/{date_today}/options/{ticker}_{date_today}.csv')
			df = pd.concat([old, ticker_obj.options]).reset_index(drop=True)
			df = df.drop_duplicates(subset=['expiration_date', 'strike_price', 'option_type'])
			df = df.sort_values(['expiration_date', 'option_type', 'strike_price'])
			df.to_csv(f"{DIR}/financial_data/{date_today}/options/{ticker}_{date_today}.csv", index=False)

			unhealthy_tickers[ticker]['new_options'] = len(df)
			delta = unhealthy_tickers[ticker]['new_options'] - unhealthy_tickers[ticker]['options']
			logger.info(f"{ticker},Re-Ticker,Success,{delta}")

		except Exception as e:

			unhealthy_tickers[ticker]['new_options'] = -1
			logger.warning(f"{ticker},Re-Ticker,Failure,{e}")

		logger.info(f"SCRAPER,RE-PROGRESS,{np.round( 100 * ((i + 1) / len(unhealthy_tickers)), 4)}%,")

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
			db_stats = [(0,0), (0,0), (0,0), (0,0)]
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

def get_unhealthies():

	def add_to_unhealthies(key, obj, unhealthies):
	    for ticker in obj:
	        try:
	            unhealthies[ticker][key] = obj[ticker]
	        except Exception as e:
	            unhealthies[ticker] = {
	                key : obj[ticker]
	            }
	    return unhealthies

	unhealthy_analysis = check_null_percentage("analysis")
	unhealthy_key_stats = check_null_percentage("key_stats")
	unhealthy_options = check_number_of_options()

	unhealthies = add_to_unhealthies("analysis", unhealthy_options, {})
	unhealthies = add_to_unhealthies("key_stats", unhealthy_key_stats, unhealthies)
	unhealthies = add_to_unhealthies("options", unhealthy_analysis, unhealthies)

	return unhealthies

def main():

	collect_data()

	unhealthies = get_unhealthies()
	if len(unhealthies) > 0:
		unhealthies = collect_data_again(unhealthies)

	success, failure = log_scraper_health()
	db_flag, db_stats, indexing_faults = database_and_alerts()
	send_scraping_report(success, failure, unhealthy_tickers, db_flag, db_stats, indexing_faults)

if __name__ == '__main__':

	logger.info(f"SCRAPER,JOB,INITIATED,{date_today}")

	init_folders()
	main()

	logger.info(f"SCRAPER,JOB,TERMINATED,{date_today}")
