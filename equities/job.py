from const import DIR, DATA, DATE, CONFIG, logger, _connector
from batch import main as batch_main
from store import main as store
from ticker import Ticker
from report import report
import sys, os

sys.path.append(f"{DIR}/../utils")
from gcp import send_gcp_metric

###################################################################################################

BATCH_SIZE = 50
N_USD = 1350
N_CAD = 150

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
		"key_stats" : len(tickers) - success['key_stats'],
		"analysis" : len(tickers) - success['analysis']
	}

	return success, failure

def send_metrics(success, failure):

	for key in success:
		metric = success[key]
		metric /= success[key] + failure[key]
		send_gcp_metric(CONFIG, f"oscrap_{key}_sucess", "double_value", metric)

def init_folders():

	DATA.mkdir()
	(DATA / "options").mkdir()
	(DATA / "ohlc").mkdir()
	(DATA / "keystats").mkdir()
	(DATA / "analysis").mkdir()

def main():

	logger.info(f"SCRAPER,JOB,INITIATED,{DATE},")

	init_folders()
	tickers = _connector.get_equity_tickers(N_USD, N_CAD)

	checkpoint = len(tickers) / BATCH_SIZE
	checkpoint = int(checkpoint / 4)

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

		if batch_id % checkpoint == 0 and batch_id != 0:
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