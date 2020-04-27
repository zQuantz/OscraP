from const import CONFIG, DIR

import sqlalchemy as sql
import pandas as pd
import os

DATE = CONFIG['date']

def index_instruments():

	dfs = []
	dir_ = f'{DIR}/instrument_data/{DATE}/'
	for file in os.listdir(dir_):
		if '.log' in file: continue
		dfs.append(pd.read_csv(dir_+file))

	df = pd.concat(dfs).dropna()
	df = df.sort_values('market_cap', ascending=False)
	df = df[df.market_cap >= 1_000]

	engine = sql.create_engine(CONFIG['db_address'])
	with engine.connect() as conn:

		ticker_codes = df.ticker + ' ' + df.exchange_code
		ticker_codes = ticker_codes.values.tolist()

		query = sql.text(f"""
			DELETE FROM
				instruments
			WHERE
				CONCAT(ticker, " ", exchange_code) in :ticker_codes
			"""
		)
		query = query.bindparams(ticker_codes=ticker_codes)
		conn.execute(query)

		df['last_updated'] = DATE
		df.to_sql("instruments", conn, if_exists="append", index=False)

		query = "SELECT * FROM instruments"
		df = pd.read_sql(query, conn)
		df = df.sort_values('market_cap', ascending=False)
		df = df.reset_index(drop=True)

	return df
