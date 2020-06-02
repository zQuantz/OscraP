from argparse import ArgumentParser
from google.cloud import storage
from scipy.stats import norm
import sqlalchemy as sql
import tarfile as tar
import pandas as pd
import numpy as np
import sys, os
import shutil
import json
import time

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))
with open(f'{DIR}/../config.json', 'r') as file:
	CONFIG = json.loads(file.read())

DBNAME = "compour9_finance"
CONFIG['db'] = DBNAME

db_address = "mysql://{user}:{password}@{ip}:{port}/{db}"
db_address = db_address.format(user=CONFIG['db_user'], password=CONFIG['db_password'],
							   ip=CONFIG['db_ip'], port=CONFIG['db_port'], db=CONFIG['db'])

ENGINE = sql.create_engine(db_address)
BUCKET = storage.Client().bucket(CONFIG['gcp_bucket_name'])

###################################################################################################

def download():

	for i, blob in enumerate(BUCKET.list_blobs()):
		
		name = blob.name
		if 'equities/' not in name:
			continue

		print("Processing:", name)

		name = os.path.basename(name)
		date_name = name.split('.')[0]

		blob.download_to_filename(f'{DIR}/tmp/{name}')

		print("Delete Old")
		try:
			shutil.rmtree(f'{DIR}/old/{date_name}')
		except Exception as e:
			print(e)

		print("Make Dir")
		try:
			os.mkdir(f'{DIR}/old/{date_name}')
		except Exception as e:
			print(e)

		print("Extracting")
		try:
			with tar.open(f'{DIR}/tmp/{name}', 'r:xz') as tar_file:
				tar_file.extractall(path=f'{DIR}/old/{date_name}')
		except Exception as e:
			print(e)

		print("\n\n")

def flatten():

	for folder in os.listdir(f"{DIR}/old"):
		for file in os.listdir(f"{DIR}/old/{folder}"):

			if not os.path.isdir(f"{DIR}/old/{folder}/{file}"):
				continue

			print(f"Flattening: {folder}")
			for subfile in os.listdir(f"{DIR}/old/{folder}/{file}"):
				os.rename(f"{DIR}/old/{folder}/{file}/{subfile}",
						  f"{DIR}/old/{folder}/{subfile}")

			os.rmdir(f"{DIR}/old/{folder}/{file}")

def initdirs():

	try:
		shutil.rmtree(f"{DIR}/new")
		shutil.rmtree(f"{DIR}/old")
		shutil.rmtree(f"{DIR}/tmp")
	except Exception as e:
		print(e)

	try:
		os.mkdir(f"{DIR}/new")
		os.mkdir(f"{DIR}/old")
		os.mkdir(f"{DIR}/tmp")
	except Exception as e:
		print(e)

def initrates():

	query = """
		SELECT
			*
		FROM
			rates
	"""

	conn = ENGINE.connect()
	rates = pd.read_sql(query, conn)
	rates['date_current'] = rates.date_current.astype(str)
	conn.close()

	dates = pd.DataFrame(os.listdir(f"{DIR}/old"), columns = ["date_current"])
	rates = dates.merge(rates, how="outer", on="date_current")
	rates['date_current'] = pd.to_datetime(rates.date_current)
	rates = rates.sort_values('date_current')
	rates = rates.fillna(method='ffill')
	rates['date_current'] = rates.date_current.astype(str)
	rates = rates.set_index('date_current') / 100
	rates = rates.T.to_dict('list')

	t_map = [
		0,
		30,
		60,
		90,
		180,
		12 * 30,
		24 * 30,
		36 * 30,
		60 * 30,
		72 * 30,
		120 * 30,
		240 * 30,
		360 * 30
	]
	t_map = np.array(t_map) / 360

	return {
		"rates" : rates,
		"t_map" : t_map
	}

def collect_ohlc():

	ohlcs = []
	for folder in os.listdir(f'{DIR}/old'):
		for file in os.listdir(f'{DIR}/old/{folder}'):
			
			if file != "ohlc.csv":
				continue

			ohlcs.append(pd.read_csv(f'{DIR}/old/{folder}/{file}'))

	ohlc = pd.concat(ohlcs)
	dates = pd.DataFrame(os.listdir(f"{DIR}/old"), columns = ["date_current"])

	ohlc = dates.merge(ohlc, how="outer", on="date_current")
	ohlc['date_current'] = pd.to_datetime(ohlc.date_current)
	ohlc = ohlc.sort_values(['date_current', 'ticker'])

	ohlc['date_current'] = ohlc.date_current.astype(str)
	return ohlc[['date_current', 'ticker', 'adj_close', 'dividend_yield']]

def transform(rates, ohlc_map):

	def drop_by_na(pkey, df, key='value'):

		def select_nna(x): 
			
			nn = x.value.notnull()
			if nn.sum() > 0:
				return x.loc[nn, key].values[0]
			else:
				return x[key].values[0]

		df = df.groupby(pkey).apply(lambda x: select_nna(x))
		return df.rename(key).reset_index(drop=False)

	for folder in sorted(os.listdir(f'{DIR}/old')):

		try:
			os.mkdir(f'{DIR}/new/{folder}')
		except Exception as e:
			print(e)

		for file in sorted(os.listdir(f'{DIR}/old/{folder}')):

			print(folder, file)

			if file == "ohlc.csv":

				shutil.copy(f'{DIR}/old/{folder}/{file}', f'{DIR}/new/{folder}/{file}')

			elif file == "options.csv":

				options = pd.read_csv(f'{DIR}/old/{folder}/{file}')
				if 'delta' in options.columns:
					options = options.drop(['delta', 'gamma', 'theta', 'vega', 'rho'], axis=1)

				if 'expiration_date' not in options.columns:
					expd = options.option_id.str.split(' ', expand=True)[1]
					options['expiration_date'] = expd

				cols = ['ticker', 'date_current', 'option_id', 'expiration_date']
				cols += [col for col in options.columns if col not in cols]
				options = options[cols]

				start = time.time()
				options = calculate_greeks(options, rates, ohlc_map)
				options.to_csv(f'{DIR}/new/{folder}/options.csv', index=False)

			elif file == "analysis.csv":
				
				df = pd.read_csv(f'{DIR}/old/{folder}/{file}')
				pkey = ["ticker", "date_current", "category", "feature", "feature_two", "modifier"]
				df.loc[:, pkey] = df[pkey].fillna('')
			
				print(file, len(df), df.ticker.nunique())
				df = drop_by_na(pkey, df)
				print(file, len(df), df.ticker.nunique())
				df.to_csv(f"{DIR}/new/{folder}/analysis.csv", index=False)
			
			elif file == "key_stats.csv":

				df = pd.read_csv(f'{DIR}/old/{folder}/{file}')
				pkey = ["ticker", "date_current", "feature", "modifier"]
				df.loc[:, pkey] = df[pkey].fillna('')
				
				print(file, len(df), df.ticker.nunique())
				df = drop_by_na(pkey, df)
				print(file, len(df), df.ticker.nunique())
				df.to_csv(f"{DIR}/new/{folder}/key_stats.csv", index=False)

		print()

def calculate_greeks(options, rates, ohlc_map):

	def get_rate(t):

		if t >= 30:
			return r_map[-1]
		
		b1 = t_map <= t
		b2 = t_map > t

		r1 = r_map[b1][-1]
		r2 = r_map[b2][0]

		t1 = t_map[b1][-1]
		t2 = t_map[b2][0]
		
		interpolated_rate = (t - t1) / (t2 - t1)
		interpolated_rate *= (r2 - r1)

		return interpolated_rate + r1

	print("Before # of Tickers:", options.ticker.nunique())
	options = options.merge(ohlc_map, how="inner", on=["ticker", "date_current"])
	print("After # of Tickers:", options.ticker.nunique())

	date = options.date_current.unique()
	print("Unique Dates:", len(date))
	date = date[0]

	r_map = [0] + rates['rates'][date]
	r_map = np.array(r_map)
	t_map = rates['t_map']

	time_to_expirations = options.time_to_expiry.unique()	
	unique_rates = {
		tte : get_rate(tte)
		for tte in time_to_expirations
	}
	options['rate'] = options.time_to_expiry.map(unique_rates)

	###############################################################################################

	o = options.copy()
	m = o.option_type.map({"C" : 1, "P" : -1})

	eqt = np.exp(-o.dividend_yield * o.time_to_expiry)
	kert = o.strike_price * np.exp(-o.rate * o.time_to_expiry)

	d1 = np.log(o.adj_close / o.strike_price)
	d1 += (o.rate - o.dividend_yield + 0.5 * (o.implied_volatility ** 2)) * o.time_to_expiry
	d1 /= o.implied_volatility * np.sqrt(o.time_to_expiry)
	d2 = d1 - o.implied_volatility * np.sqrt(o.time_to_expiry)

	delta = m * eqt * norm.cdf(m * d1)

	gamma = eqt * norm.pdf(d1)
	gamma /= (o.adj_close * o.implied_volatility * np.sqrt(o.time_to_expiry))

	vega = o.adj_close * eqt * norm.pdf(d1) * np.sqrt(o.time_to_expiry)	

	rho = m * o.time_to_expiry * kert * norm.cdf(m * d2)

	theta = (o.adj_close * norm.pdf(m * d1) * o.implied_volatility)
	theta *= -eqt / (2 * np.sqrt(o.time_to_expiry))
	theta -= m * o.rate * kert * norm.cdf(m * d2)
	theta += m * o.dividend_yield * o.adj_close * eqt * norm.cdf(m * d1)

	###############################################################################################

	options['delta'] = delta
	options['gamma'] = gamma
	options['theta'] = theta / 365
	options['vega'] = vega / 100
	options['rho'] = rho / 100

	cols = ['delta', 'gamma', 'theta', 'vega', 'rho']
	options[cols] = options[cols].replace([-np.inf, np.inf], np.nan)
	options[cols] = options[cols].fillna(0).round(6)

	options = options.drop(['adj_close', 'dividend_yield', 'rate'], axis=1)

	print("Null values")
	print(options[cols].isnull().sum())

	return options

def drop_tables():

	print("Dropping Tables")

	conn = ENGINE.connect()

	statements = [
		"DROP TABLE options;",
		"DROP TABLE ohlc;",
		"DROP TABLE key_stats;",
		"DROP TABLE analysis;",
	]

	for statement in statements:

		max_tries = 5
		tries = 0

		while tries < max_tries:
		
			try:
				print("Executing", statement, tries)
				resp = conn.execute(statement)
				break
			except Exception as e:
				print(e)

			tries += 1

	print()

	conn.close()

def create_tables():

	conn = ENGINE.connect()

	statements = [
		CONFIG['oscrap_options_table_structure'].format(TABLE_NAME="options"),
		CONFIG['oscrap_ohlc_table_structure'].format(TABLE_NAME="ohlc"),
		CONFIG['oscrap_key_stats_table_structure'].format(TABLE_NAME="key_stats"),
		CONFIG['oscrap_analysis_table_structure'].format(TABLE_NAME="analysis")
	]

	for statement in statements:

		max_tries = 5
		tries = 0
		
		while tries < max_tries:
		
			try:
				print("Executing", statement, tries)
				resp = conn.execute(statement)
				break
			except Exception as e:
				print(e)

			tries += 1
	
	print()

	conn.close()

def index_tables():

	folders = sorted(os.listdir(f"{DIR}/new"))
	idx = folders.index("2020-03-16")
	for folder in folders[idx:]:
		
		for file in sorted(os.listdir(f"{DIR}/new/{folder}")):

			max_tries = 5
			tries = 0
			
			while tries < max_tries:
			
				try:

					print("Indexing:", folder, file)

					table_name = file[:-4]
					df = pd.read_csv(f"{DIR}/new/{folder}/{file}")

					conn = ENGINE.connect()
					df.to_sql(name=table_name, con=conn, if_exists='append', index=False, chunksize=100_000)
					conn.close()

					break

				except Exception as e:
					
					print(e)

				tries += 1

			if tries >= max_tries:
				raise Exception(f"Too many tries. {folder},{file}")

		print()

if __name__ == '__main__':

	# initdirs()
	# download()
	# flatten()

	# rates = initrates()
	# ohlc_map = collect_ohlc()
	# transform(rates, ohlc_map)

	# drop_tables()
	# create_tables()
	# index_tables()