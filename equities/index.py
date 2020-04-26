from const import DIR, CONFIG

import sqlalchemy as sql
import pandas as pd
import os

###################################################################################################

DB_ADDRESS = CONFIG['db_address']
DATE = CONFIG['date']

###################################################################################################

def index(tickers):

	engine = sql.create_engine(DB_ADDRESS)

	with engine.connect() as conn:

		options_pre = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
		ohlc_pre = conn.execute("SELECT COUNT(*) FROM ohlc;").fetchone()[0]
		analysis_pre = conn.execute("SELECT COUNT(*) FROM analysis;").fetchone()[0]
		key_stats_pre = conn.execute("SELECT COUNT(*) FROM key_stats;").fetchone()[0]

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

		options_post = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
		ohlc_post = conn.execute("SELECT COUNT(*) FROM ohlc;").fetchone()[0]
		analysis_post = conn.execute("SELECT COUNT(*) FROM analysis;").fetchone()[0]
		key_stats_post = conn.execute("SELECT COUNT(*) FROM key_stats;").fetchone()[0]

	return [(options_pre, options_post), (ohlc_pre, ohlc_post), (analysis_pre, analysis_post), (key_stats_pre, key_stats_post)]