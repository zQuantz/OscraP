from const import DIR, date_today
import sqlalchemy as sql
import pandas as pd
import numpy as np
import os

def send_to_database():

	def binarize(x):
	    q = np.quantile(x, 0.25)
	    return not (x.values[-1] >= q)[0]

	engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_test")

	with engine.connect() as conn:

		options_pre = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
		equities_pre = conn.execute("SELECT COUNT(*) FROM equities;").fetchone()[0]
		analysis_pre = conn.execute("SELECT COUNT(*) FROM analysis;").fetchone()[0]
		key_stats_pre = conn.execute("SELECT COUNT(*) FROM key_stats;").fetchone()[0]

		options = []
		equities = []
		analysis = []
		key_stats = []

		for file in os.listdir(f'{DIR}/financial_data/{date_today}/options'):

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/financial_data/{date_today}/options/{file}')
			df['ticker'] = ticker
			df['option_id'] = (df.ticker + ' ' + df.expiration_date + ' ' + df.option_type
							  + np.round(df.strike_price, 2).astype(str))
			options.append(df)

		for file in os.listdir(f'{DIR}/financial_data/{date_today}/equities'):

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/financial_data/{date_today}/equities/{file}')
			df['ticker'] = ticker

			equities.append(df.iloc[:1, :])

		for file in os.listdir(f'{DIR}/financial_data/{date_today}/analysis'):

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/financial_data/{date_today}/analysis/{file}')
			df['ticker'] = ticker
			df['date_current'] = date_today
			analysis.append(df)

		for file in os.listdir(f'{DIR}/financial_data/{date_today}/key_stats'):

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/financial_data/{date_today}/key_stats/{file}')
			df['ticker'] = ticker
			df['date_current'] = date_today
			key_stats.append(df)

		options = pd.concat(options)
		options.to_sql(name='options', con=conn, if_exists='append', index=False, chunksize=10_000)

		equities = pd.concat(equities)
		equities.to_sql(name='equities', con=conn, if_exists='append', index=False, chunksize=10_000)

		analysis = pd.concat(analysis)
		analysis.to_sql(name='analysis', con=conn, if_exists='append', index=False, chunksize=10_000)

		key_stats = pd.concat(key_stats)
		key_stats.to_sql(name='key_stats', con=conn, if_exists='append', index=False, chunksize=10_000)

		options_post = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
		equities_post = conn.execute("SELECT COUNT(*) FROM equities;").fetchone()[0]
		analysis_post = conn.execute("SELECT COUNT(*) FROM analysis;").fetchone()[0]
		key_stats_post = conn.execute("SELECT COUNT(*) FROM key_stats;").fetchone()[0]

	return [(options_pre, options_post), (equities_pre, equities_post), (analysis_pre, analysis_post), (key_stats_pre, key_stats_post)]