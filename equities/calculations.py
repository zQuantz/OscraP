from collections import defaultdict

from scipy.stats import norm
import pandas as pd
import numpy as np
import sys, os

###################################################################################################

SURFACE_COLS = []
for e in [1,3,6,9,12,18,24]:
	for m in range(80,125,5):
		SURFACE_COLS.append(f"m{e}m{m}")

###################################################################################################

def greeks(options):

	o = options.copy()
	m = o.option_type.map({"C" : 1, "P" : -1}).values

	tau = o.days_to_expiry.values / 365
	rtau = np.sqrt(tau)
	iv = o.implied_volatility.values
	S = o.stock_price.values
	K = o.strike_price.values
	q = o.dividend_yield.values
	r = o.rate.values

	###################################################################################################

	eqt = np.exp(-q * tau)
	kert = K * np.exp(-r * tau)

	d1 = np.log(S / K)
	d1 += (r - q + 0.5 * (iv ** 2)) * tau
	d1 /= iv * rtau
	d2 = d1 - iv * rtau

	npd1 = norm.pdf(d1)
	ncd1 = norm.cdf(m * d1)
	ncd2 = norm.cdf(m * d2)

	###################################################################################################

	delta = m * eqt * ncd1

	gamma = np.exp(q - r) * npd1
	gamma /= (S * iv * rtau)

	vega = S * eqt * npd1 * rtau	
	vega /= 100

	rho = m * tau * kert * ncd2
	rho /= 100

	theta = (S * norm.pdf(m * d1) * iv)
	theta *= -eqt / (2 * rtau)
	theta -= m * r * kert * ncd2
	theta += m * q * S * eqt * ncd1
	theta /= 365

	###################################################################################################

	vanna = (vega / S)
	vanna *= (1 - d1 / (iv * rtau))

	vomma = (vega / iv) * (d1 * d2)

	charm = 2 * (r - q) * tau - d2 * iv * rtau
	charm /= 2 * tau * iv * rtau
	charm *= eqt * npd1
	charm = m * q * eqt * ncd1 - charm
	charm /= 365

	veta = q.copy()
	veta += ((r - q) * d1) / (iv * rtau)
	veta -= (1 + d1 * d2) / (2 * tau)
	veta *= -S * eqt * npd1 * rtau
	veta /= 365 * 100

	speed = 1
	speed += d1 / (iv * rtau)
	speed *= -gamma / S

	zomma = (d1 * d2 - 1) / iv
	zomma *= gamma

	color = 2 * (r - q) * tau
	color -= d2 * iv * rtau
	color *= d1 / (iv * rtau)
	color += 2 * q * tau + 1
	color *= -eqt * npd1 / (2 * S * tau * iv * rtau)
	color /= 365

	ultima = d1 * d2 * (1 - d1 * d2) + d1 * d1 + d2 * d2
	ultima *= -vega / (iv * iv)

	###################################################################################################

	options['delta'] = delta
	options['gamma'] = gamma
	options['theta'] = theta
	options['vega'] = vega
	options['rho'] = rho

	options['vanna'] = vanna
	options['vomma'] = vomma
	options['charm'] = charm
	options['veta'] = veta
	options['speed'] = speed
	options['zomma'] = zomma
	options['color'] = color
	options['ultima'] = ultima

	options.loc[:, greek_columns] = options[greek_columns].replace([-np.inf, np.inf], np.nan)
	options.loc[:, greek_columns] = options[greek_columns].round(6).fillna(0)

	return options

def closest_surface(options, ohlc, date):

	def by_ticker(df):

		expirations = np.array([1,3,6,9,12,18,24]) / 12
		expirations = expirations.reshape(1, -1)

		moneynesses = np.arange(0.8, 1.25, 0.05)
		moneynesses = moneynesses.reshape(-1, 1)
		
		T = df.days_to_expiry
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

	start = date
	end = f"{int(start[:4]) + 4}-{start[4:]}"

	fridays = pd.date_range(start, end, freq="WOM-3FRI").astype(str)
	thursdays = pd.date_range(start, end, freq="WOM-3THU").astype(str)
	regulars = list(fridays) + list(thursdays)

	ohlc = ohlc[['date_current', 'ticker', 'adjclose_price']]
	options = options.merge(ohlc, on=['date_current', 'ticker'], how="inner")
	options['years'] = options.days_to_expiry / 365
	
	options['moneyness'] = options.strike_price / options.adjclose_price
	options['otm'] = options.adjclose_price - options.strike_price
	options['otm'] = options.otm * options.option_type.map({"C" : 1, "P" : -1})

	options = options[options.otm < 0]
	options = options[options.expiration_date.astype(str).isin(regulars)]

	surface = options.groupby(["date_current", "ticker"]).apply(by_ticker)
	surface = surface.reset_index()
	
	label = "m" + surface.expiration.astype(str)
	label += "m" + surface.moneyness.astype(str)
	surface['label'] = label
	
	surface = surface.pivot(index="ticker", values="iv", columns="label")
	surface = surface[label.unique()].reset_index()
	surface['date_current'] = date
	
	return surface

def synth_surface(options, ohlc, date):

	def brackets(reference, target, values):
	
		reference = reference.repeat(len(target), axis=0)
		
		diff = np.subtract(reference, target)
		sign = np.sign(diff)
		diffsign = np.diff(sign, axis=1)

		idc = np.argwhere(diffsign)
		midc = idc[:, 0]

		sidc = idc[:, 1]
		sidc = np.stack([sidc, sidc+1], axis=1).reshape(-1, )

		vm = values[sidc].reshape(-1, 2)
		vw = diff[midc.repeat(2), sidc].reshape(-1, 2)

		vw[vw == 0] = 1e-6
		vw = 1 / abs(vw)
		vw = vw / vw.sum(axis=1, keepdims=True)
		
		return vm, vw, midc, sidc

	def by_ticker(df):

		def by_expiration(e):

			moneynesses = np.arange(0.8, 1.25, 0.05)
			moneynesses = moneynesses.reshape(-1, 1)

			e = e.sort_values("moneyness")
			ivs = e.implied_volatility.values
			m = e.moneyness.values.reshape(1, -1)
			
			vm, vw, midc, sidc = brackets(m, moneynesses, ivs)
			
			surface_ivs = np.array([np.nan]*9)
			surface_ivs[midc] = (vm * vw).sum(axis=1)

			cols = [f"m{int(v * 100)}" for v in moneynesses]
			return pd.DataFrame([surface_ivs], columns = cols)

		mivs = df.groupby("expiration_date", as_index=False).apply(by_expiration).values

		expirations = np.array([1,3,6,9,12,18,24]) / 12
		expirations = expirations.reshape(1, -1)

		T = df.days_to_expiry
		T = (T.unique() / 365).reshape(-1, 1)
		
		vm, vw, midc, sidc = brackets(T.T, expirations.T, df.days_to_expiry.values)

		tivs = mivs[sidc] * vw.reshape(-1, 1)
		tivs = tivs.reshape(-1, 2, 9).round(2).sum(axis=1)

		surfacem = np.ones((7, 9))
		surfacem[:, :] = np.nan
		surfacem[midc, :] = tivs
		surfacem = surfacem.reshape(-1, )
		
		return pd.DataFrame(surfacem).T

	###############################################################################################

	start = date
	end = f"{int(start[:4]) + 4}-{start[4:]}"

	fridays = pd.date_range(start, end, freq="WOM-3FRI").astype(str)
	thursdays = pd.date_range(start, end, freq="WOM-3THU").astype(str)
	regulars = list(fridays) + list(thursdays)

	ohlc = ohlc[['date_current', 'ticker', 'adjclose_price']]
	options = options.merge(ohlc, on=['date_current', 'ticker'], how="inner")
	options['years'] = options.days_to_expiry / 365
	
	options['moneyness'] = options.strike_price / options.adjclose_price
	options['otm'] = options.adjclose_price - options.strike_price
	options['otm'] = options.otm * options.option_type.map({"C" : 1, "P" : -1})

	options = options[options.otm < 0]
	options = options[options.expiration_date.astype(str).isin(regulars)]

	cols = ["date_current", "ticker"]
	surface_df = options.groupby(cols).apply(by_ticker)
	surface_df = surface_df.reset_index().drop("level_2", axis=1)
	surface_df.columns = cols + SURFACE_COLS
	
	return surface_df
	