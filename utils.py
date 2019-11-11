import pandas as pd
import numpy as np


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