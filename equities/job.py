from const import DIR, DATA, DATE, CONFIG, logger, _connector
import pandas_market_calendars as mcal
from batch import main as batch_main
from store import main as store
from ticker import Ticker
from report import report
import pandas as pd
import sys, os

sys.path.append(f"{DIR}/../utils")
from gcp import send_gcp_metric

###################################################################################################

BATCH_SIZE = 2
N_USD = 10

###################################################################################################

def get_job_success_rates(tickers):

	success = {
		"options" : len(os.listdir(DATA / "options")),
		"ohlc" : len(os.listdir(DATA / "ohlc")),
		"keystats" : len(os.listdir(DATA / "keystats")),
		"analysis" : len(os.listdir(DATA / "analysis"))
	}

	failure = {
		"options" : len(tickers) - success['options'],
		"ohlc" : len(tickers) - success['ohlc'],
		"keystats" : len(tickers) - success['keystats'],
		"analysis" : len(tickers) - success['analysis']
	}

	return success, failure

def send_metrics(success, failure):

	for key in success:
		metric = success[key]
		metric /= success[key] + failure[key]
		# send_gcp_metric(CONFIG, f"oscrap_{key}_sucess", "double_value", metric)

def init():

	DATA.mkdir()
	(DATA / "options").mkdir()
	(DATA / "ohlc").mkdir()
	(DATA / "keystats").mkdir()
	(DATA / "analysis").mkdir()

	nyse = mcal.get_calendar('NYSE')
	min_date = DATE
	max_date = f"{int(DATE[:4])+5}"+DATE[4:]

	schedule = nyse.schedule(start_date=min_date, end_date=max_date)
	trading_days = mcal.date_range(schedule, frequency="1D").tolist()
	CONFIG['trading_days'] = [str(day)[:10] for day in trading_days]

	fridays = pd.date_range(min_date, max_date, freq="WOM-3FRI").astype(str)
	thursdays = pd.date_range(min_date, max_date, freq="WOM-3THU").astype(str)
	CONFIG['reg_expirations'] = list(fridays) + list(thursdays)

	CONFIG['ratemap'] = _connector.get_ratemap()

	_connector.init_date_series()

def main():

	logger.info(f"SCRAPER,JOB,INITIATED,{DATE},")

	init()

	tickers = _connector.get_equity_tickers(N_USD)
	checkpoint = len(tickers) / BATCH_SIZE
	checkpoint = int(checkpoint / 4)

	faults_summary = {
		"options" : {},
		"analysis" : {},
		"keystats" : {},
		"ohlc" : {}
	}

	db_flags, db_stats = [], []

	###############################################################################################

	for batch_id, batch in enumerate(range(BATCH_SIZE, len(tickers) + BATCH_SIZE, BATCH_SIZE)):

		ticker_batch = tickers[batch - BATCH_SIZE : batch]

		results = batch_main(batch_id, ticker_batch)
		b_fault_summary, b_db_flag, b_db_stats = results

		for key in b_fault_summary:
			for ticker in b_fault_summary[key]:
				faults_summary[key][ticker] = b_fault_summary[key][ticker]

		db_flags.append(b_db_flag)
		db_stats.append(b_db_stats)

		success, failure = get_job_success_rates(tickers[ : BATCH_SIZE * (1 + batch_id)])
		send_metrics(success, failure)

		if batch_id % checkpoint == 0 and batch_id != 0:
			report("Partial", success, failure, faults_summary, db_flags, db_stats)

	###############################################################################################

	success, failure = get_job_success_rates(tickers)
	report("Full", success, failure, faults_summary, db_flags, db_stats)

	store()

	logger.info(f"SCRAPER,JOB,TERMINATED,{DATE},")

if __name__ == '__main__':

	try:
	
		# send_gcp_metric(CONFIG, "oscrap_job_status", "int64_value", 1)
		main()
	
	except Exception as e:

		# send_gcp_metric(CONFIG, "oscrap_job_status", "int64_value", 0)
		logger.warning(f"SCRAPER,JOB,MAIN ERROR,{e},")
