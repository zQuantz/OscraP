from const import DIR, logger, CONFIG

from batch import main as batch_main
from store import main as store
from datetime import datetime
from ticker import Ticker
from report import report
from uuid import getnode
import sqlalchemy as sql
import pandas as pd
import sys, os

###################################################################################################

DATE = CONFIG['date']
BATCH_SIZE = 25
N_USD = 900
N_CAD = 100

###################################################################################################

def get_job_success_rates(tickers):

	collected_options = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{DATE}/options')]
	collected_ohlc = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{DATE}/ohlc')]
	collected_analysis = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{DATE}/analysis')]
	collected_key_stats = [file.split('_')[0] for file in os.listdir(f'{DIR}/financial_data/{DATE}/key_stats')]

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

	for ticker in tickers:

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

	return success, failure

def fetch_tickers():

	engine = sql.create_engine(CONFIG['db_address'])
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

	usd = df[~df.exchange_code.isin(["TSX"])].iloc[:N_USD, :]
	cad = df[df.exchange_code.isin(["TSX"])].iloc[:N_CAD, :]
	tickers = (usd.ticker.values.tolist() + cad.ticker.values.tolist())

	df = df[df.ticker.isin(tickers)]
	df = df.sort_values('market_cap', ascending=False)

	return tuple(df.ticker)

def init_folders():

	os.mkdir(f'{DIR}/financial_data/{DATE}')
	os.mkdir(f'{DIR}/financial_data/{DATE}/options')
	os.mkdir(f'{DIR}/financial_data/{DATE}/ohlc')
	os.mkdir(f'{DIR}/financial_data/{DATE}/key_stats')
	os.mkdir(f'{DIR}/financial_data/{DATE}/analysis')

def main():

	logger.info(f"SCRAPER,JOB,INITIATED,{DATE},")

	init_folders()
	tickers = fetch_tickers()
	
	midpoint = len(tickers) / BATCH_SIZE
	midpoint = int(midpoint / 2)

	faults_summary = {
		"options" : {},
		"analysis" : {},
		"key_stats" : {},
		"ohlc" : {}
	}

	db_flags, db_stats = [], []
	indexing_attempts = []

	###############################################################################################

	for batch_id, batch in enumerate(range(BATCH_SIZE, len(tickers) + BATCH_SIZE, BATCH_SIZE)):

		ticker_batch = tickers[batch - BATCH_SIZE : batch]

		results = batch_main(batch_id, ticker_batch)
		b_fault_summary, b_db_flag, b_db_stats, b_indexing_attempt = results

		for key in b_fault_summary:
			for ticker in b_fault_summary[key]:
				faults_summary[key][ticker] = b_fault_summary[key][ticker]

		db_flags.append(b_db_flag)
		db_stats.append(b_db_stats)
		indexing_attempts.append(b_indexing_attempt)

		if batch_id == midpoint:
			success, failure = get_job_success_rates(tickers)
			report("Partial", success, failure, faults_summary, db_flags, db_stats, indexing_attempts)

	###############################################################################################

	success, failure = get_job_success_rates(tickers)
	report("Full", success, failure, faults_summary, db_flags, db_stats, indexing_attempts)

	store()

	logger.info(f"SCRAPER,JOB,TERMINATED,{DATE},")

if __name__ == '__main__':

	main()
