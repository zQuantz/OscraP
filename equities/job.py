from const import DIR, logger, CONFIG

from batch import main as batch_main
from store import main as store
from datetime import timedelta
from datetime import datetime
from ticker import Ticker
from report import report
import sqlalchemy as sql
import pandas as pd
import numpy as np
import sys, os

sys.path.append(f"{DIR}/../utils")
from send_gcp_metric import send_gcp_metric

###################################################################################################

DATE = CONFIG['date']
BATCH_SIZE = 50
N_USD = 900
N_CAD = 100

###################################################################################################

def get_job_success_rates(tickers):

	success = {
		"options" : len(os.listdir(f'{DIR}/financial_data/{DATE}/options')),
		"ohlc" : len(os.listdir(f'{DIR}/financial_data/{DATE}/ohlc')),
		"key_stats" : len(os.listdir(f'{DIR}/financial_data/{DATE}/key_stats')),
		"analysis" : len(os.listdir(f'{DIR}/financial_data/{DATE}/analysis'))
	}

	failure = {
		"options" : len(tickers) - success['options'],
		"ohlc" : len(tickers) - success['ohlc'],
		"key_stats" : len(tickers) - success['key_stats'],
		"analysis" : len(tickers) - success['analysis']
	}

	return success, failure

def send_metrics(success, failure):

	for key in success:
		metric = success[key]
		metric /= success[key] + failure[key]
		send_gcp_metric(CONFIG, f"oscrap_{key}_sucess", "double_value", metric)

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

def fetch_rates():

	engine = sql.create_engine(CONFIG['db_address'])

	date = datetime.now() - timedelta(days=7)
	date = date.strftime("%Y-%m-%d")

	query = f"""
		SELECT
			*
		FROM
			rates
		WHERE
			date_current >= "{date}"
	"""
	
	rates = pd.read_sql(query, engine)
	rates = rates.iloc[-1, :]

	t_map = [
		0,
		30,
		60,
		90,
		180,
		12 * 30,
		24 * 30,
		36 * 30,
		60 * 30,
		72 * 30,
		120 * 30,
		240 * 30,
		360 * 30
	]
	t_map = np.array(t_map) / 360
	r_map = [0] + list(rates.values[1:] / 100)
	r_map = np.array(r_map)

	return {
		"t_map" : t_map,
		"r_map" : r_map,
		"rates" : {}
	}

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
	CONFIG['rates'] = fetch_rates()

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

		success, failure = get_job_success_rates(tickers[ : BATCH_SIZE * (1 + batch_id)])
		send_metrics(success, failure)

		if batch_id == midpoint:
			report("Partial", success, failure, faults_summary, db_flags, db_stats, indexing_attempts)

	###############################################################################################

	success, failure = get_job_success_rates(tickers)
	report("Full", success, failure, faults_summary, db_flags, db_stats, indexing_attempts)

	store()

	logger.info(f"SCRAPER,JOB,TERMINATED,{DATE},")

if __name__ == '__main__':

	try:
		main()
	except Exception as e:
		logger.warning(f"SCRAPER,JOB,MAIN ERROR,{e},")