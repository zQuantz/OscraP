from const import DIR, CONFIG, logger

from collections import defaultdict

import sqlalchemy as sql
import pandas as pd
import numpy as np
import sys, os

###################################################################################################

start = CONFIG['date']
end = f"{int(start[:4]) + 4}-{start[4:]}"

fridays = pd.date_range(start, end, freq="WOM-3FRI").astype(str)
thursdays = pd.date_range(start, end, freq="WOM-3THU").astype(str)
regulars = list(fridays) + list(thursdays)

typemap = {"C" : 1, "P" : -1}

###################################################################################################

def pre_surface(options):

	def by_ticker(df):

		expirations = np.array([1,3,6,9,12,18,24]) / 12
		expirations = expirations.reshape(1, -1)

		moneynesses = np.arange(0.8, 1.25, 0.05)
		moneynesses = moneynesses.reshape(-1, 1)
		
		T = (df.expiration_date - df.date_current).dt.days
		T = (T.unique() / 365).reshape(-1, 1)
		expirations = np.repeat(expirations, len(T), axis=0)

		diff = abs(np.subtract(expirations, T))
		idc = np.argmin(diff, axis=0)
		T = T[idc].reshape(-1, )

		yearmap = defaultdict(list)
		for k, v in zip(T, expirations[0]):
			yearmap[k].append( int(12 * v) )
		yearmap = dict(yearmap)

		df['expiration'] = df.years.map(yearmap)
		df = df.explode('expiration').dropna()

		def by_expiration(e):

			e = e.sort_values("moneyness")
			m = e.moneyness.values.reshape(1, -1)
			m = m.repeat(len(moneynesses), axis=0)

			ivs = e.implied_volatility.values

			diff = np.subtract(m, moneynesses)
			sign = np.sign(diff)
			idc = np.argwhere(np.diff(sign))
			idc = {k : v for k, v in zip(idc[:, 0], idc[:, 1])}

			surface = []

			for i in range(len(moneynesses)):

				if i in idc:

					idx = idc[i]

					weights = m[i][idx:idx+2]
					weights -= moneynesses[i]
					weights[weights == 0] = 1e-6

					weights = 1 / abs(weights)
					weights = weights / weights.sum()

					iv = (ivs[idx:idx+2] * weights).sum()

				elif sign[i][0] == 1:

					iv = ivs[0]

				else:

					iv = ivs[-1]

				surface.append([
					int(100 * moneynesses[i][0]),
					iv
				])

			return pd.DataFrame(surface, columns = ['moneyness', 'iv'])

		return df.groupby('expiration').apply(by_expiration)

	ohlc = ohlc[['date_current', 'ticker', 'adj_close']]
	options = options.merge(ohlc, on=['date_current', 'ticker'], how="inner")

	delta = (options.expiration_date - options.date_current)
	options['days'] = delta.dt.days
	options['years'] = delta.dt.days / 365
	
	options['moneyness'] = options.strike_price / options.adj_close
	options['otm'] = options.adj_close - options.strike_price
	options['otm'] = options.otm * options.option_type.map(typemap)

	options = options[options.otm < 0]
	options = options[options.expiration_date.astype(str).isin(regulars)]

	surface = options.groupby(["date_current", "ticker"]).apply(by_ticker)
	surface = surface.reset_index()

	cols = ['date_current', 'ticker', 'expiration', 'iv']
    time_surface = surface[cols]
    time_surface = time_surface.groupby(cols[:-1], as_index = False).mean()
    time_surface = time_surface.pivot(index="ticker", columns="expiration", values="iv")
    
    time_surface.columns = [f"m{e}" for e in time_surface.columns]
    time_surface = time_surface.reset_index()
    time_surface['date_current'] = CONFIG['date']
	
	label = "m" + surface.expiration.astype(str)
	label += "m" + surface.moneyness.astype(str)
	surface['label'] = label
	
	surface = surface.pivot(index="ticker", values="iv", columns="label")
	surface = surface[label.unique()].reset_index()
	surface['date_current'] = CONFIG['date']
	
	return surface, time_surface