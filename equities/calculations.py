from const import DATE, CONFIG

import pandas as pd
import numpy as np
import sys, os

from scipy.optimize import brentq
from scipy.stats import norm

from datetime import datetime, timedelta

DATE_FMT = "%Y-%m-%d"

###################################################################################################

def calculate_trading_days(d1, d2, tdays):

	if d1 in tdays and d2 in tdays:
		return tdays.index(d2) - tdays.index(d1)

	_d2 = datetime.strftime(d2, DATE_FMT)
	ctr = 0
	
	while d2 not in tdays:
		
		_d2 -= timedelta(days=1)
		d2 = _d2.strftime(DATE_FMT)
		ctr += 1

		if ctr > 3:
			return None

	return tdays.index(d2) - tdays.index(d1)

def calculate_regular_expiries(d1, d2):

	saturdays = pd.date_range(d1, d2, freq="WOM-3SAT").astype(str)
	fridays = pd.date_range(d1, d2, freq="WOM-3FRI").astype(str)
	thursdays = pd.date_range(d1, d2, freq="WOM-3THU").astype(str)
	return list(saturdays) + list(fridays) + list(thursdays)

def calculate_greeks(options):

	o = options.copy()
	m = o.option_type.map({"C" : 1, "P" : -1}).values

	tau = o.days_to_expiry.values / 252
	rtau = np.sqrt(tau)
	iv = o.zimplied_volatility.values
	S = o.stock_price.values
	K = o.strike_price.values
	q = np.log(1 + o.dividend_yield.values / 100)
	r = np.log(1 + o.rate.values / 100)

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
	theta /= 252

	###################################################################################################

	vanna = (vega / S)
	vanna *= (1 - d1 / (iv * rtau))

	vomma = (vega / iv) * (d1 * d2)

	charm = 2 * (r - q) * tau - d2 * iv * rtau
	charm /= 2 * tau * iv * rtau
	charm *= eqt * npd1
	charm = m * q * eqt * ncd1 - charm
	charm /= 252

	veta = q.copy()
	veta += ((r - q) * d1) / (iv * rtau)
	veta -= (1 + d1 * d2) / (2 * tau)
	veta *= -S * eqt * npd1 * rtau
	veta /= 252 * 100

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
	color /= 252

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

###################################################################################################

time_anchors = [1,2,3,6,9,12,18,24]
moneyness_anchors = list(range(80, 125, 5))

TDF_COLS = ['expiration'] + [f"m{m}" for m in range(80, 125, 5)]
MDF_COLS = ['moneyness', 'm1', 'm2', 'w1', 'w2', 'ziv1', 'ziv2', 'iv1', 'iv2']
SURFACE_COLS = [
	f"m{m1}m{m2}"
	for m1 in time_anchors
	for m2 in moneyness_anchors
]

time_anchors = np.array(time_anchors) * 21
time_df = pd.DataFrame(time_anchors, columns = ['expiration'])

moneyness_anchors = np.array(moneyness_anchors)
moneyness_df = pd.DataFrame(moneyness_anchors, columns = ['moneyness'])

def calculate_surface(options, reg_expirations):

	def pre_filters(options):

		options = options[options.bid_price != 0]
		options = options[options.ask_price != 0]
		options = options[options.zimplied_volatility != 0]
		return options[options.days_to_expiry > 5]

	def post_filters(options):

		ticker_exp = options.ticker + " " + options.expiration_date
		x = ticker_exp.value_counts()
		x = x[x > 1]

		return options[ticker_exp.isin(x.index)]

	def calculate_implied_forward(options):

		def by_ticker(ticker_exp):

			cols = ['strike_price', 'mid_price']
			calls = ticker_exp[ticker_exp.option_type == "C"][cols]
			puts = ticker_exp[ticker_exp.option_type != "C"][cols]

			if len(calls) == 0 or len(puts) == 0:
				return None

			prices = calls.merge(puts, on="strike_price", how="outer")
			prices = prices.reset_index(drop=True)

			diff = (prices.mid_price_x - prices.mid_price_y)
			idc = diff.abs().argmin()

			r = np.log(1 + ticker_exp.rate.values[0] / 100)
			T = ticker_exp.days_to_expiry.values[0] / 252
			K = ticker_exp.strike_price.values[idc]

			return K + np.exp(-r * T) * diff.iloc[idc]

		cols = ["ticker", "expiration_date"]
		forwards = options.groupby(cols).apply(by_ticker)
		forwards = forwards.reset_index(name="F")
		return options.merge(forwards, on=cols, how="inner").dropna()

	def brackets(values, anchors):
	    
	    N = len(values)
	    M = len(anchors)
	    
	    values = np.array(values)
	    matrix = values.repeat(M).reshape(N, M)
	    
	    matrix -= anchors
	    signed_matrix = np.sign(matrix)
	    dsigned_matrix = np.diff(signed_matrix, axis=0)
	    
	    return matrix, signed_matrix, dsigned_matrix

	def calculate_bracket_coords(values, anchors, idx, match_type, extra_values=None):
		
		v = anchors[idx[1]]
		v1 = values[idx[0]]

		if match_type == 0:
			idx2 = idx[0] + 1
			bump = 0
		else:
			idx2 = idx[0]
			bump = 1
		
		v2 = values[idx2]

		p1 = 1 / abs(v1 - v + bump)
		p2 = 1 / abs(v2 - v + bump)
		d = p1 + p2

		p1 /= d
		p2 /= d

		coords = [v, v1, v2, p1, p2]

		if extra_values is not None:
			coords.extend([
				extra_values[0][idx[0]],
				extra_values[0][idx2],
				extra_values[1][idx[0]],
				extra_values[1][idx2],
			])

		return coords

	def calculate_brackets(values, anchors, sm, dsm, extra_values=None):

		brackets = [
			calculate_bracket_coords(values, anchors, idx, 0, extra_values)
			for idx in np.argwhere(dsm == 2)
		]

		brackets.extend([
			calculate_bracket_coords(values, anchors, idx, 1, extra_values)
			for idx in np.argwhere(sm == 0)
		])

		return brackets

	def by_ticker(options):

		expirations = options[options.expiration_date.isin(reg_expirations)]
		expirations = expirations.days_to_expiry.unique()

		if len(expirations) < 2:
			return pd.DataFrame(columns = SURFACE_COLS)

		m, sm, dsm = brackets(expirations, time_anchors)
		time_brackets = calculate_brackets(expirations, time_anchors, sm, dsm)

		idc = np.argwhere(dsm.sum(axis=0) == 0)
		if len(idc) != 0:

			idc = idc.reshape(-1, )
			expirations = options.days_to_expiry.unique()
			m, sm, dsm = brackets(expirations, time_anchors[idc])
			time_brackets.extend(
				calculate_brackets(expirations, time_anchors[idc], sm, dsm)
			)

		if len(time_brackets) == 0:
			return

		time_brackets = np.array(sorted(time_brackets))
		expirations = np.unique(time_brackets[:, 1:3].reshape(-1, ))

		zivs, ivs = {}, {}
		for expiration in expirations:

			_options = options[options.days_to_expiry == expiration]
			_options = _options.sort_values("moneyness", ascending=True)

			moneyness = _options.moneyness.values
			ziv = _options.zimplied_volatility.values
			iv = _options.implied_volatility.values

			m, sm, dsm = brackets(moneyness, moneyness_anchors)
			moneyness_brackets = calculate_brackets(
				moneyness,
				moneyness_anchors,
				sm,
				dsm,
				[ziv, iv]
			)

			if len(moneyness_brackets) == 0:
				zivs[expiration] = np.ones(len(moneyness_df)) * np.nan
				ivs[expiration] = np.ones(len(moneyness_df)) * np.nan
				continue 

			df = pd.DataFrame(moneyness_brackets, columns = MDF_COLS)
			df['ziv'] = df.ziv1 * df.w1 + df.ziv2 * df.w2
			df['iv'] = df.iv1 * df.w1 + df.iv2 * df.w2
			
			zdf = df[['moneyness', 'ziv']]
			zdf = zdf.groupby("moneyness").mean().reset_index()
			zdf = moneyness_df.merge(zdf, on='moneyness', how='outer')

			df = df[['moneyness', 'iv']]
			df = df.groupby("moneyness").mean().reset_index()
			df = moneyness_df.merge(df, on='moneyness', how='outer')

			zivs[expiration] = zdf.ziv.values
			ivs[expiration] = df.iv.values

		###########################################################################################

		surface = [
			[b[0]] + (zivs[b[1]] * b[3] + zivs[b[2]] * b[4]).tolist()
			for b in time_brackets
		]
		surface = pd.DataFrame(surface, columns = TDF_COLS)
		surface = time_df.merge(surface, on="expiration", how="outer")
		surface = surface.values[:, 1:].reshape(1, -1)
		zsurface = pd.DataFrame(surface, columns = SURFACE_COLS)
		zsurface['method'] = 'in_house'

		surface = [
			[b[0]] + (ivs[b[1]] * b[3] + ivs[b[2]] * b[4]).tolist()
			for b in time_brackets
		]
		surface = pd.DataFrame(surface, columns = TDF_COLS)
		surface = time_df.merge(surface, on="expiration", how="outer")
		surface = surface.values[:, 1:].reshape(1, -1)
		surface = pd.DataFrame(surface, columns = SURFACE_COLS)
		surface['method'] = 'yahoo'

		return pd.concat([zsurface, surface], axis=0)

	###############################################################################################

	options = options[options.days_to_expiry > 0].reset_index(drop=True)
	options['mid_price'] = (options.bid_price * 0.5 + options.ask_price * 0.5).values

	options = pre_filters(options)	
	options = calculate_implied_forward(options)

	omap = options.option_type.map({
		"C" : 1,
		"P" : -1
	})	
	options = options[omap * (options.F - options.strike_price) <= 0]
	options['moneyness'] = options.strike_price / options.F * 100

	options = post_filters(options)

	cols = ["ticker", "days_to_expiry", "option_type", "strike_price"]
	options = options.sort_values(cols)

	surface = options.groupby("ticker").apply(by_ticker)
	surface = surface.dropna(axis=0, how="all", subset=SURFACE_COLS)
	
	zsurface = surface[surface.method == 'in_house'].reset_index(level=0)
	zsurface = zsurface.drop("method", axis=1)

	surface = surface[surface.method != 'in_house'].reset_index(level=0)
	surface = surface.drop("method", axis=1)

	return zsurface, surface

def calculate_iv(options):

	def bs_price(v, S, K, T, r, q, t, M):
	
		d1 = np.log(S / K) + (r + 0.5 * v * v) * T
		d1 /= np.sqrt(T) * v
		d2 = d1 - np.sqrt(T) * v

		return t * (S * np.exp(-q * T) * norm.cdf(t * d1) - K * np.exp(-r * T) * norm.cdf(t * d2))

	def root(v, *args):
		return bs_price(v, *args) - args[-1]

	Ss = options.stock_price.values
	Ks = options.strike_price.values
	Ts = options.days_to_expiry.values / 252
	rs = options.rate.values / 100
	qs = options.dividend_yield.values / 100
	ts = options.option_type.map({"C" : 1, "P" : -1}).values
	Ms = ((options.bid_price + options.ask_price) / 2).values

	zivs = []
	values = zip(Ss, Ks, Ts, rs, qs, ts, Ms)
	for S, K, T, r, q, t, M in values:
		try:
			iv = brentq(root, 0, 10_000, args=(S, K, T, r, q, t, M), maxiter=10_000)
		except Exception as e:
			iv = 0
		zivs.append(round(iv * 100, 4))

	options['zimplied_volatility'] = zivs

	return options

if __name__ == '__main__':

	min_date = "2019-01-01"
	max_date = "2030-01-01"
	fridays = pd.date_range(min_date, max_date, freq="WOM-3FRI").astype(str)
	thursdays = pd.date_range(min_date, max_date, freq="WOM-3THU").astype(str)
	CONFIG['reg_expirations'] = list(fridays) + list(thursdays)

	df = pd.read_csv("financial_data/2020-12-30/6.csv")
	surface = calculate_surface(df)
