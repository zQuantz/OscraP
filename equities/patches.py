from const import DIR, date_today, option_cols, option_new_cols, equity_cols, equity_new_cols, logger
import sqlalchemy as sql
import pandas as pd
import numpy as np
import sys, os
import pickle
import shutil

def format_option_chain(df):

	info_cols = ['CurrentDate', 'Open', 'High', 'Low', 'Close', 'StockVolume', 'DividendYield', 'StrikePrice', 'ExpirationDate']
	call_cols = ['OptionType', 'ExpirationDate', 'TimeToExpiry', 'OpenInterest', 'Volume', 'Bid', 'Ask', 'OptionPrice', 'ImpliedVolatility', 'StrikePrice']
	put_cols = ['StrikePrice', 'ImpliedVolatility', 'OptionPrice', 'Bid', 'Ask', 'Volume', 'OpenInterest', 'OptionType', 'ExpirationDate']

	I = df[info_cols].drop_duplicates()
	C = df[df.OptionType == 'C']
	C = C[call_cols]
	P = df[df.OptionType == 'P']
	P = P[put_cols]

	rcols = ['StrikePrice', 'ExpirationDate', 'TimeToExpiry']
	P.columns = [col+'_P' if col not in rcols else col for col in P.columns]
	C.columns = [col+'_C' if col not in rcols  else col for col in C.columns]
	call_cols = C.columns.tolist()
	put_cols = P.columns.tolist()

	info_cols = info_cols[:-2]
	call_cols = call_cols[:-1]
	put_cols = put_cols[1:-1]

	ndf = I.merge(C, how='inner', on=['ExpirationDate', 'StrikePrice']).merge(P, how='inner', on=['ExpirationDate', 'StrikePrice'])
	ndf = ndf[info_cols + call_cols + ['StrikePrice'] + put_cols]
	ndf = ndf.sort_values(['ExpirationDate','StrikePrice']).dropna()
	ndf.columns = [col.split('_')[0] for col in ndf.columns]

	return ndf

def reformat_all_folders():
	"""
	Shouldnt be used on already formatted folders. I added a safe check that protects against an empty dataframe after twice formatting.
	"""

	for folder in os.listdir(f"{DIR}/options_data"):
		
		if os.path.isdir(f"{DIR}/options_data/{folder}"):
			
			for file in os.listdir(f"{DIR}/options_data/{folder}"):
				
				ext = file.split('.')[-1]
				if ext == 'txt':
					continue

				df = pd.read_csv(f"{DIR}/options_data/{folder}/{file}")
				cols = df.columns
				cols = [col if col != 'ExpierationDate' else 'ExpirationDate' for col in cols]
				df.columns = cols

				if 'OptionType.1' in df.columns:
					continue
				
				print(folder, file)
				df = format_option_chain(df)
				df.to_csv(f"{DIR}/options_data/{folder}/{file}", index=False)

			try:
				os.remove(f"{DIR}/options_data/{folder}.zip")
			except Exception as e:
				print("Folder", folder, "not fold, Error:", e)
			shutil.make_archive(f"{DIR}/options_data/{folder}", "zip", f"{DIR}/options_data/{folder}")

def reformat_all_folders_again():
	"""
	Shouldnt be used on already formatted folders. I added a safe check that protects against an empty dataframe after twice formatting.
	"""

	for folder in os.listdir(f"{DIR}/options_data"):
		
		if os.path.isdir(f"{DIR}/options_data/{folder}"):
			
			for file in os.listdir(f"{DIR}/options_data/{folder}"):
				
				ext = file.split('.')[-1]
				if ext == 'txt':
					continue

				df = pd.read_csv(f"{DIR}/options_data/{folder}/{file}")
				new_cols = df.columns.tolist()
				
				idx_oi = new_cols.index("TimeToExpiry")
				idx_ot = new_cols.index("OptionType")
				new_cols[idx_oi] = "OptionType"
				new_cols[idx_ot] = "OpenInterest"

				idx_oi = new_cols.index("Bid.1")
				idx_ot = new_cols.index("Ask.1")
				new_cols[idx_oi] = "Ask.1"
				new_cols[idx_ot] = "Bid.1"

				df = df[new_cols]


				new_rows = []
				for row in df.values:
				    tmp_c, tmp_p = [], []
				    i = row[0:9]
				    c = row[9:17]
				    p = row[16:25]
				    p = p[::-1]
				    assert len(c) == len(p)
				    
				    tmp_c.extend(i.tolist())
				    tmp_c.extend(c.tolist())
				    
				    tmp_p.extend(i.tolist())
				    tmp_p.extend(p.tolist())
				    
				    new_rows.append(tmp_c)
				    new_rows.append(tmp_p)

				ndf = pd.DataFrame(new_rows)
				ndf.columns = new_cols[:-7]
				df = ndf.sort_values(["ExpirationDate", "OptionType", "StrikePrice"])

				df.to_csv(f"{DIR}/options_data/{folder}/{file}", index=False)

			try:
				os.remove(f"{DIR}/options_data/{folder}.zip")
			except Exception as e:
				print("Folder", folder, "not fold, Error:", e)
			shutil.make_archive(f"{DIR}/options_data/{folder}", "zip", f"{DIR}/options_data/{folder}")

def convert_IV_to_percentage():
	"""
	Shouldnt be used on already formatted files
	"""

	for folder in os.listdir(f"{DIR}/options_data"):
		
		if os.path.isdir(f"{DIR}/options_data/{folder}"):
			
			for file in os.listdir(f"{DIR}/options_data/{folder}"):
				
				ext = file.split('.')[-1]
				if ext == 'txt':
					continue

				df = pd.read_csv(f"{DIR}/options_data/{folder}/{file}")
				cols = df.columns
				cols = [col if col != 'ExpierationDate' else 'ExpirationDate' for col in cols]
				df.columns = cols

				
				if df['ImpliedVolatility'].mean() < 3:
					continue

				print(folder, file)
				for col in df.columns:
					if 'ImpliedVolatility' in col:
						df[col] = df[col] / 100
				df.to_csv(f"{DIR}/options_data/{folder}/{file}", index=False)

			try:
				os.remove(f"{DIR}/options_data/{folder}.zip")
			except Exception as e:
				print("Folder", folder, "not fold, Error:", e)
			shutil.make_archive(f"{DIR}/options_data/{folder}", "zip", f"{DIR}/options_data/{folder}")

def data_to_database():

	print("Sending data to SQL.")

	engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_finance")
	conn = engine.connect()

	conn.execute('DELETE FROM options;')
	conn.execute('DELETE FROM equities;')

	folders = os.listdir(f'{DIR}/options_data')
	folders = sorted(folders)
	for folder in folders:

		if not os.path.isdir(f'{DIR}/options_data/{folder}'):
			continue

		print("\n\n\n- - - - - - - - - -  - - - - - -")
		print("Processing folder:", folder)

		options = []
		equities = []

		for file in os.listdir(f'{DIR}/options_data/{folder}'):

			if '.txt' in file:
				continue

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/options_data/{folder}/{file}')
			df['Ticker'] = ticker
			df['OptionID'] = (df.Ticker + ' ' + df.ExpirationDate + ' ' + df.OptionType 
							  + np.round(df.StrikePrice, 2).astype(str))

			opts = df[option_cols]
			opts.columns = option_new_cols
			options.append(opts)

			eqts = df[equity_cols]
			eqts.columns = equity_new_cols
			equities.append(eqts.iloc[:1, :])

			print(f"Processing {ticker} for database ingestion.")

		if len(options) == 0:
			return

		options = pd.concat(options)
		options.to_sql(name='options', con=conn, if_exists='append', index=False, chunksize=10_000)

		equities = pd.concat(equities)
		equities.to_sql(name='equities', con=conn, if_exists='append', index=False, chunksize=10_000)

	conn.close()

def fix_broken_column_names():

	sorted_cols = [
		'CurrentDate',
 		'Open',
 		'High',
 		'Low',
 		'Close',
		'AdjClose',
		'StockVolume',
		'DividendYield',
		'ExpirationDate',
		'TimeToExpiry',
		'OptionType',
		'StrikePrice',
		'Bid',
		'OptionPrice',
		'Ask',
		'ImpliedVolatility',
		'OpenInterest',
		'Volume'
	]

	for folder in os.listdir(f"{DIR}/options_data"):
		
		if os.path.isdir(f"{DIR}/options_data/{folder}"):
			
			for file in os.listdir(f"{DIR}/options_data/{folder}"):
				
				print(folder, file)

				ext = file.split('.')[-1]
				if ext == 'txt':
					continue

				df = pd.read_csv(f"{DIR}/options_data/{folder}/{file}")

				if 'OpenInterest.1' in df.columns:

					df['AdjClose'] = df.Close
					df = df.drop('OpenInterest.1', axis=1)

					expiration_days = np.busday_count(df.CurrentDate, df.ExpirationDate)
					expiration_days = np.round(expiration_days / 252, decimals=6)
					expiration_days = np.array([expiration_days, np.zeros(len(expiration_days))]).T
					df['TimeToExpiry'] = np.max(expiration_days, axis=1)

				df = df[sorted_cols]
				df.to_csv(f"{DIR}/options_data/{folder}/{file}", index=False)

			try:
				os.remove(f"{DIR}/options_data/{folder}.zip")
			except Exception as e:
				print("Folder", folder, "not fold, Error:", e)
			shutil.make_archive(f"{DIR}/options_data/{folder}", "zip", f"{DIR}/options_data/{folder}")

def create_ticker_dict():

	tickers = {}

	with open(f'{DIR}/data/NYSE.txt', 'r') as file:
		for line in file:
			line = line.split('\t')
			if '-' in line[0] or '.' in line[0]:
				continue
			tickers[line[0]] = line[1][:-1]

	with open(f'{DIR}/data/NASDAQ.txt', 'r') as file:
		for line in file:
			line = line.split('\t')
			if '-' in line[0] or '.' in line[0]:
				continue
			tickers[line[0]] = line[1][:-1]

	df = pd.read_csv(f'{DIR}/data/most_traded_etfs.csv')

	for row in df.values:
		tickers[row[0]] = row[1]

	tickers_to_scrape = {}

	with open(f'{DIR}/data/merged_ticker_list.txt', 'r') as file:
		for line in file:
			ticker = line.replace('\n', '')
			try:
				tickers_to_scrape[ticker] = tickers[ticker]
			except:
				tickers_to_scrape[ticker] = 'No Description.'

	for row in df.values[:30]:
		tickers_to_scrape[row[0]] = row[1]

	with open(f'{DIR}/data/tickers.pickle', 'wb') as file:
		pickle.dump(tickers_to_scrape, file)
