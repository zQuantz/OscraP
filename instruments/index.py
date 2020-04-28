from const import CONFIG, DIR

from mysql import connector
import pandas as pd
import os

###################################################################################################

DATE = CONFIG['date']
DATE = "2020-03-31"

###################################################################################################

def index_instruments():

	dfs = []
	dir_ = f'{DIR}/instrument_data/{DATE}/'
	for file in os.listdir(dir_):
		if '.log' in file: continue
		dfs.append(pd.read_csv(dir_+file))

	df = pd.concat(dfs).dropna()
	df = df.sort_values('market_cap', ascending=False)
	df = df[df.market_cap >= 1_000]

	try:

		conn = connector.connect(**CONFIG['bluehost_database'])

		ticker_codes = df.ticker + ' ' + df.exchange_code
		ticker_codes = tuple(ticker_codes.values)

		query = f"""
			DELETE FROM
				instruments
			WHERE
				CONCAT(ticker, " ", exchange_code) in {ticker_codes};
			"""

		cursor = conn.cursor()
		cursor.execute(query)

		df['last_updated'] = DATE
		df.to_sql("compour9_test.instruments", conn, if_exists="append", index=False)

		print("This.")

		query = "SELECT * FROM instruments"
		df = pd.read_sql(query, conn)
		df = df.sort_values('market_cap', ascending=False)
		df = df.reset_index(drop=True)

	except Exception as e:

		conn.close()
		raise Exception(f"MySQL Connection Error - {e}")

	conn.close()

	return df

def index(parallel_log):

	max_attempts = 5
	indexing_attempts = 0

	while indexing_attempts < max_attempts:

		try:

			df = index_instruments()
			break

		except Exception as e:

			parallel_log(f"Index Fail. {e}")
			indexing_attempts += 1

	if indexing_attempts >= max_attempts:
		raise Exception("Too Many Indexing Attempts.")

	return df
