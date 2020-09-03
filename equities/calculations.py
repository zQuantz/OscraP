from collections import defaultdict

from scipy.stats import norm
import pandas as pd
import numpy as np
import sys, os

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

def surface(options, ohlc, date):

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
