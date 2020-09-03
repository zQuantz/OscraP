from const import CONFIG, DIR, DATE, DATA, logger, _connector

import sqlalchemy as sql
import pandas as pd
import os

###################################################################################################

def index_instruments():

	dfs = []
	for file in DATA.iterdir():
		if '.log' in file.name:
			continue
		dfs.append(pd.read_csv(file))

	df = pd.concat(dfs, sort=False).dropna()
	df = df.sort_values('market_cap', ascending=False)
	df = df[df.market_cap >= 1_000]

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
	_connector.execute(query)

	df['last_updated'] = DATE
	_connector.write("instruments", df)

	df = _connector.read("SELECT * FROM instruments;")
	df = df.sort_values('market_cap', ascending=False)
	df = df.reset_index(drop=True)

	return df

def index():

	max_attempts = 5
	indexing_attempts = 0

	while indexing_attempts < max_attempts:

		try:

			df = index_instruments()
			break

		except Exception as e:

			logger.warning(f"Index Fail. {e}")
			indexing_attempts += 1

	if indexing_attempts >= max_attempts:
		raise Exception("Too Many Indexing Attempts.")

	return df
