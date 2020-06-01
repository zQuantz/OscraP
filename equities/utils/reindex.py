import sys
sys.path.append("..")

from const import DIR, CONFIG

from datetime import datetime, timedelta
from greeks import calculate_greeks
from argparse import ArgumentParser
from google.cloud import storage
import sqlalchemy as sql
import tarfile as tar
import pandas as pd
import numpy as np
import sys, os

def all(database):

	try:
		os.mkdir(f"{DIR}/utils/tmp")
	except Exception as e:
		print(e)

	engine = "mysql://{username}:{password}@{host}:{port}/{database}"
	engine = engine.format(username=CONFIG['db_user'], password=CONFIG['db_password'],
						   host=CONFIG['db_ip'], port=CONFIG['db_port'],
						   database=database)
	engine = sql.create_engine(engine)

	bucket = storage.Client().bucket(CONFIG['gcp_bucket_name'])
	for i, blob in enumerate(bucket.list_blobs()):
		
		if "equities/" not in blob.name:
			continue

		blob.download_to_filename(f"{DIR}/utils/current.tar.xz")
		with tar.open(f"{DIR}/utils/current.tar.xz", "r") as tar_file:
			tar_file.extractall(f"{DIR}/utils/tmp")

		for file in os.listdir(f"{DIR}/utils/tmp"):

			if '.ipy' in file:
				continue

			print(blob.name, file)

			table_name = file.split('.')[0]
			df = pd.read_csv(f"{DIR}/utils/tmp/{file}")

			file_date = df.date_current.values[0]
			folder_date = blob.name.split('.')[0]
			
			max_tries = 5
			tries = 0
	
			while tries < max_tries:
			
				try:
					
					delete_query = f"""
						DELETE FROM
							{table_name}
						WHERE
							date_current = "{folder_date}"
					"""
					
					conn = engine.connect()
					result = conn.execute(delete_query)
					print(result)
					conn.close()
					
					delete_query = f"""
						DELETE FROM
							{table_name}
						WHERE
							date_current = "{file_date}"
					"""
					
					conn = engine.connect()
					result = conn.execute(delete_query)
					print(result)
					conn.close()
					
					conn = engine.connect()
					result = df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=10_000)
					conn.close()
					
					print("Success", tries, result)
					
					break
									
				except Exception as e:
					
					print(e)
					
				tries += 1

def all_options_greeks(database):

	stock_price_cache = {}
	dividend_cache = {}

	engine = sql.create_engine(CONFIG['db_address'])

	date = datetime.now() - timedelta(days=7)
	date = date.strftime("%Y-%m-%d")

	query = f"""
		SELECT
			*
		FROM
			rates
	"""
	rates = pd.read_sql(query, engine)
	rates['date_current'] = rates.date_current.astype(str)

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

	try:
		os.mkdir(f"{DIR}/utils/tmp")
	except Exception as e:
		print(e)

	engine = "mysql://{username}:{password}@{host}:{port}/{database}"
	engine = engine.format(username=CONFIG['db_user'], password=CONFIG['db_password'],
						   host=CONFIG['db_ip'], port=CONFIG['db_port'],
						   database=database)
	engine = sql.create_engine(engine)

	bucket = storage.Client().bucket(CONFIG['gcp_bucket_name'])

	for i, blob in enumerate(bucket.list_blobs()):
		
		if "equities/" not in blob.name:
			continue

		date = os.path.basename(blob.name[:-7])

		try:

			blob.download_to_filename(f"{DIR}/utils/current.tar.xz")
			with tar.open(f"{DIR}/utils/current.tar.xz", "r") as tar_file:
				tar_file.extractall(f"{DIR}/utils/tmp")

		except Exception as e:

			print(e)

		try:

			for file in os.listdir(f"{DIR}/utils/tmp/{date}"):
				os.rename(f"{DIR}/utils/tmp/{date}/{file}", f"{DIR}/utils/tmp/{file}")

			os.rmdir(f"{DIR}/utils/tmp/{date}")

		except Exception as e:

			print(e)

		ohlc = pd.read_csv(f"{DIR}/utils/tmp/ohlc.csv")
		options = pd.read_csv(f"{DIR}/utils/tmp/options.csv")

		try:
			os.mkdir(f"{DIR}/utils/tformed/{date}")
		except Exception as e:
			print(e)

		try:
			r_map = [0] + list(rates[rates.date_current == date].values[0, 1:])
			r_map = np.array(r_map)
		except Exception as e:
			print(e)
			print(r_map)

		CONFIG['rates'] = {
			"t_map" : t_map,
			"r_map" : r_map,
			"rates" : {}
		}

		greeks = []

		for ticker in options.ticker.unique():

			tmp_options = options[options.ticker == ticker]
			tmp_ohlc = ohlc[ohlc.ticker == ticker]

			try:
				
				stock_price = tmp_ohlc.adj_close.values[0]
				div_yield = tmp_ohlc.dividend_yield.values[0]
				
				stock_price_cache[ticker] = stock_price
				dividend_cache[ticker] = div_yield

			except Exception as e:
				
				print(e)
				stock_price = stock_price_cache[ticker]
				div_yield = dividend_cache[ticker]

			tmp_options = tmp_options.dropna()
			greeks.append(calculate_greeks(stock_price, div_yield, tmp_options))

		options = pd.concat(greeks, axis=0).reset_index(drop=True)
		options.to_csv(f"{DIR}/utils/tmp/options.csv", index=False)

		for file in os.listdir(f"{DIR}/utils/tmp"):

			if '.ipy' in file:
				continue

			print(blob.name, file)

			table_name = file.split('.')[0]
			df = pd.read_csv(f"{DIR}/utils/tmp/{file}")

			if table_name == "options" and "expiration_date" not in df.columns:
				df['expiration_date'] = df.option_id.str.split(' ', expand=True)[1]
				fcols = ['ticker', 'date_current', 'option_id', 'expiration_date']
				df = df[fcols + [col for col in df.columns if col not in fcols]]
				print(df.expiration_date)

			df.to_csv(f"{DIR}/utils/tformed/{date}/{file}", index=False)

			file_date = df.date_current.values[0]
			folder_date = os.path.basename(blob.name.split('.')[0])
			
			max_tries = 5
			tries = 0
	
			while tries < max_tries:
			
				try:
					
					conn = engine.connect()
					result = df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=10_000)
					conn.close()
					
					print("Success", tries, result)
					break
									
				except Exception as e:
					
					print(e)
					
				tries += 1

		for file in os.listdir(f"{DIR}/utils/tmp"):
			os.remove(f"{DIR}/utils/tmp/{file}")

def one(folder, database):

	engine = "mysql://{username}:{password}@{host}:{port}/{database}"
	engine = engine.format(username=CONFIG['db_user'], password=CONFIG['db_password'],
						   host=CONFIG['db_ip'], port=CONFIG['db_port'],
						   database=database)
	
	engine = sql.create_engine(engine)

	for file in os.listdir(folder):

		table_name = file.split('.')[0]
		df = pd.read_csv(f"{folder}/{file}")
		file_date = df.date_current.values[0]

		max_tries = 5
		tries = 0

		print(file, table_name, file_date, os.path.basename(folder))

		while tries < max_tries:
		
			try:
				
				delete_query = f"""
					DELETE FROM
						{table_name}
					WHERE
						date_current = "{os.path.basename(folder)}"
				"""
				
				conn = engine.connect()
				result = conn.execute(delete_query)
				print(result)
				conn.close()
				
				delete_query = f"""
					DELETE FROM
						{table_name}
					WHERE
						date_current = "{file_date}"
				"""
				
				conn = engine.connect()
				result = conn.execute(delete_query)
				print(result)
				conn.close()
				
				conn = engine.connect()
				result = df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=10_000)
				conn.close()
				
				print("Success", tries, result)
				
				break
								
			except Exception as e:
				
				print(e)
				
			tries += 1
			
		print()

if __name__ == '__main__':

	all_options_greeks("compour9_finance")

	# argparser = ArgumentParser()
	# argparser.add_argument("method")
	# argparser.add_argument("folder")
	# argparser.add_argument("database")
	# args = argparser.parse_args()

	# if args.method == "one":
	# 	one(args.folder, args.database)
	# elif args.method == "all":
	# 	all(args.database)
