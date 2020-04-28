from const import DIR, CONFIG, COUNT_QUERY

import sqlalchemy as sql
import pandas as pd
import os

###################################################################################################

DB_ADDRESS = CONFIG['db_address']
DATE = CONFIG['date']

###################################################################################################

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
			options.to_sql(name='options', con=conn, if_exists='append', index=False, chunksize=10_000)

		if len(ohlc) > 0:
			ohlc = pd.concat(ohlc)
			ohlc.to_sql(name='ohlc', con=conn, if_exists='append', index=False, chunksize=10_000)

		if len(analysis) > 0:
			analysis = pd.concat(analysis)
			analysis.to_sql(name='analysis', con=conn, if_exists='append', index=False, chunksize=10_000)

		if len(key_stats) > 0:
			key_stats = pd.concat(key_stats)
			key_stats.to_sql(name='key_stats', con=conn, if_exists='append', index=False, chunksize=10_000)

		count_df['post'] = pd.read_sql(COUNT_QUERY, conn).iloc[:, 1]

	return list(map(tuple, count_df.iloc[:, 1:].values))
