from const import DIR, DATE, DATA, CONFIG, _connector, logger

from calculations import calculate_surface, calculate_iv
from ticker import Ticker
import pandas as pd
import numpy as np
import sys, os
import time

###################################################################################################

SLEEP = 2.5

###################################################################################################

def collect_data(batch_id, tickers):

	for i, ticker in enumerate(tickers):
		
		try:

			Ticker(ticker, logger, batch_id)
			time.sleep(SLEEP)
			
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
				for key in ['analysis', 'keystats', 'ohlc', 'options']
			}

			ticker_obj = Ticker(ticker, logger, batch_id, retries, faults[ticker])
			faults[ticker] = ticker_obj.fault_dict
			time.sleep(SLEEP)

			logger.info(f"{ticker},{batch_id},Re-Ticker,Success,")

		except Exception as e:

			logger.warning(f"{ticker},{batch_id},Re-Ticker,Failure,{e}")

		pct = (i + 1) / len(faults)
		pct = np.round(100 * pct, 4)
		logger.info(f"SCRAPER,{batch_id},RE-PROGRESS,{pct}%,")

	return faults

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

	def check_lower_bounds(tickers, product):

		lower_bounds = _connector.get_lower_bounds(f"{product}counts", batch_id)
		lower_bounds = lower_bounds.set_index("ticker")
		lower_bounds = lower_bounds.astype(int).to_dict()['lower_bound']

		unhealthy = {}
		for ticker in tickers:

			if ticker not in lower_bounds:
				continue

			file = (DATA / product / f"{ticker}_{DATE}.csv")
			if file.exists():
				
				df = pd.read_csv(file)
				if len(df) <= lower_bounds[ticker]:

					unhealthy[ticker] = {
						"lower_bound" : lower_bounds[ticker],
						"old" : len(df),
						"new" : 0
					}

			else:

				unhealthy[ticker] = {
					"lower_bound" : lower_bounds[ticker],
					"old" : 0,
					"new" : 0
				}

		return unhealthy

	def check_ohlc(tickers):

		tickers = _connector.get_distinct_ohlc_tickers(batch_id).ticker

		collected = [
			ticker.split("_")[0]
			for ticker in os.listdir(f"{DATA}/ohlc")
		]

		unhealthy = {}
		for ticker in tickers:
			
			if ticker not in collected:

				unhealthy[ticker] = {
					"status" : 0,
					"new_status" : 0
				}

		return unhealthy

	try:

		analysis_faults = check_lower_bounds(tickers, "analysis")
		keystats_faults = check_lower_bounds(tickers, "keystats")
		options_faults = check_lower_bounds(tickers, "options")
		ohlc_faults = check_ohlc(tickers)

		logger.info(f"SCRAPER,{batch_id},FAULTS,SUCCESS,")

	except Exception as e:

		logger.info(f"SCRAPER,{batch_id},FAULTS,FAILURE,{e}")

	faults = add_to_faults("analysis", analysis_faults, {})
	faults = add_to_faults("keystats", keystats_faults, faults)
	faults = add_to_faults("options", options_faults, faults)
	faults = add_to_faults("ohlc", ohlc_faults, faults)
	faults = collect_data_again(batch_id, faults)

	faults_summary = {
	    key : {}
	    for key in ["analysis", "keystats", "ohlc", "options"]
	}

	for ticker in faults:
	    for key in faults[ticker]:
	        faults_summary[key][ticker] = faults[ticker][key]

	return faults_summary

def index_data(batch_id, tickers):

	try:

		options, ohlc = [], []
		analysis, keystats = [], []

		for file in (DATA/"options").iterdir():

			ticker = file.name.split('_')[0]
			if ticker not in tickers:
				continue

			options.append(pd.read_csv(file))

		for file in (DATA/"ohlc").iterdir():

			ticker = file.name.split('_')[0]
			if ticker not in tickers:
				continue

			ohlc.append(pd.read_csv(file).iloc[:1, :])

		for file in (DATA/"analysis").iterdir():

			ticker = file.name.split('_')[0]
			if ticker not in tickers:
				continue

			analysis.append(pd.read_csv(file))

		for file in (DATA / "keystats").iterdir():

			ticker = file.name.split('_')[0]
			if ticker not in tickers:
				continue
				
			keystats.append(pd.read_csv(file))

		pre = _connector.get_equities_table_count().row_count

		if len(options) > 0:
			options = pd.concat(options)
			_connector.write("options", options)

		if len(ohlc) > 0:
			ohlc = pd.concat(ohlc)
			_connector.write("ohlc", ohlc)

		if len(analysis) > 0:
			_connector.write("analysis", pd.concat(analysis))

		if len(keystats) > 0:
			_connector.write("keystats", pd.concat(keystats))

		if len(options) > 0 and len(ohlc) > 0:

			cols = ["date_current", "ticker", "adjclose_price"]
			options = options.merge(ohlc[cols], on=cols[:2], how="inner")
			options = options.rename({"adjclose_price" : "stock_price"}, axis=1)
			options = options.merge(CONFIG['ratemap'], on="days_to_expiry", how="inner")

			surface = calculate_surface(options, CONFIG['reg_expirations'])
			surface['date_current'] = DATE

			info = f"{surface.ticker.nunique()} / {options.ticker.nunique()}"
			logger.info(f"SCRAPER,{batch_id},SURFACE,{info}")
			
			_connector.write("surface", surface)

		post = _connector.get_equities_table_count().row_count

		db_stats = (pre.tolist(), post.tolist())
		db_flag = 1
		
		logger.info(f"SCRAPER,{batch_id},INDEXING,SUCCESS,")

	except Exception as e:

		logger.warning(f"SCRAPER,{batch_id},INDEXING,FAILURE,{e}")
		print_exc()
		
		db_stats = ([0]*4, [0]*4)
		db_flag = 0

	return db_flag, db_stats

def main(batch_id, tickers):

	logger.info(f"SCRAPER,{batch_id},INITIATED,,")

	collect_data(batch_id, tickers)

	_connector.init_batch_tickers(batch_id, tickers)

	faults_summary = fix_faults(batch_id, tickers)

	db_flag, db_stats = index_data(batch_id, tickers)

	_connector.launch_derived_engine(batch_id)

	logger.info(f"SCRAPER,{batch_id},TERMINATED,,")

	return faults_summary, db_flag, db_stats
