from const import DIR, DATE, DATA, CONFIG, _connector
from calculations import surface
import pandas as pd
import os

###################################################################################################

def index(tickers):

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

	count_df = _connector.get_equities_table_count()
	count_df.columns = ['table', 'pre']

	if len(options) > 0:
		_connector.write("optionsBACK", pd.concat(options))

	if len(ohlc) > 0:
		_connector.write("ohlcBACK", pd.concat(ohlc))

	if len(analysis) > 0:
		_connector.write("analysisBACK", pd.concat(analysis))

	if len(keystats) > 0:
		_connector.write("keystatsBACK", pd.concat(keystats))

	if len(options) > 0 and len(ohlc) > 0:
		_connector.write("surfaceBACK", surface(options, ohlc, DATE))

	count_df['post'] = _connector.get_equities_table_count().count

	return list(map(tuple, count_df.iloc[:, 1:].values))
