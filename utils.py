from const import DIR
import pandas as pd
import numpy as np
import sys, os
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