from const import DIR, DATE, DATA, CONFIG, logger, _connector
import pandas as pd
import numpy as np
import sys, os

###################################################################################################

def check_count_quantiles(tickers, product):

	df = _connector.get_data_counts(f"{product}counts")
	quantiles = df.groupby('ticker').apply(
			lambda x: np.quantile(x['count'].values, 0.25)
		)
	quantiles = quantiles.astype(int).to_dict()

	for ticker in tickers:
		if ticker not in quantiles:
			quantiles[ticker] = 0

	unhealthy = {}
	for ticker in quantiles:

		file = (DATA / product / f"{ticker}_{DATE}.csv")

		if not file.exists() or len(pd.read_csv(file)) <= quantiles[ticker]:

			unhealthy[ticker] = {
				'quantile' : quantiles[ticker],
				'old' : len(df),
				'new' : 0
			}

	return unhealthy

def check_ohlc(tickers):

	tickers = _connector.get_distinct_ohlc_tickers().ticker

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
