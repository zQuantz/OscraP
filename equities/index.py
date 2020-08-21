from const import DIR, CONFIG, COUNT_QUERY
from precompute import precompute

import sqlalchemy as sql
import pandas as pd
import os

###################################################################################################

DB_ADDRESS = CONFIG['db_address']
DATE = CONFIG['date']

###################################################################################################

def to_sql(df, table_name, conn):
	df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=10_000)

def index(tickers):

	options, ohlc = [], []
	analysis, key_stats = [], []

	for file in os.listdir(f'{DIR}/financial_data/{DATE}/options'):

		ticker = file.split('_')[0]
		if ticker not in tickers:
			continue

		df = pd.read_csv(f'{DIR}/financial_data/{DATE}/options/{file}')
		df['ticker'] = ticker
		df['option_id'] = (df.ticker + ' ' + df.expiration_date + ' ' + df.option_type
						  + df.strike_price.round(2).astype(str))
		options.append(df)

	for file in os.listdir(f'{DIR}/financial_data/{DATE}/ohlc'):

		ticker = file.split('_')[0]
		if ticker not in tickers:
			continue

		ticker = file.split('_')[0]
		df = pd.read_csv(f'{DIR}/financial_data/{DATE}/ohlc/{file}')
		df['ticker'] = ticker

		ohlc.append(df.iloc[:1, :])

	for file in os.listdir(f'{DIR}/financial_data/{DATE}/analysis'):

		ticker = file.split('_')[0]
		if ticker not in tickers:
			continue

		ticker = file.split('_')[0]
		df = pd.read_csv(f'{DIR}/financial_data/{DATE}/analysis/{file}')
		df['ticker'] = ticker
		df['date_current'] = DATE
		analysis.append(df)

	for file in os.listdir(f'{DIR}/financial_data/{DATE}/key_stats'):

		ticker = file.split('_')[0]
		if ticker not in tickers:
			continue

		ticker = file.split('_')[0]
		df = pd.read_csv(f'{DIR}/financial_data/{DATE}/key_stats/{file}')
		df['ticker'] = ticker
		df['date_current'] = DATE
		key_stats.append(df)

	with sql.create_engine(DB_ADDRESS).connect() as conn:

		count_df = pd.read_sql(COUNT_QUERY, conn)
		count_df.columns = ['table', 'pre']

		if len(options) > 0:
			options = pd.concat(options)
			to_sql(options, "options", conn)

		if len(ohlc) > 0:
			ohlc = pd.concat(ohlc)
			to_sql(ohlc, "ohlc", conn)

		if len(analysis) > 0:
			analysis = pd.concat(analysis)
			to_sql(analysis, "analysis", conn)

		if len(key_stats) > 0:
			key_stats = pd.concat(key_stats)
			to_sql(key_stats, "key_stats", conn)

		if len(options) > 0 and len(ohlc) > 0:
			
			surface, tickerdates_query, tickeroids_query = precompute(options, ohlc)
			
			to_sql(surface, "surface", conn)
			
			conn.execute(tickerdates_query)
			conn.execute(tickeroids_query)

		count_df['post'] = pd.read_sql(COUNT_QUERY, conn).iloc[:, 1]

	return list(map(tuple, count_df.iloc[:, 1:].values))
