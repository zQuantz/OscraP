from const import DATE, CONFIG

import pandas as pd
import numpy as np
import sys, os

from scipy.optimize import brentq
from scipy.stats import norm

###################################################################################################

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

def calculate_surface(options):

	time_anchors = [1,3,6,9,12,18,24]
	time_anchors = np.array(time_anchors) * 21
	time_df = pd.DataFrame(time_anchors, columns = ['expiration'])

	moneyness_anchors = list(range(80, 125, 5))
	moneyness_anchors = np.array(moneyness_anchors)
	moneyness_df = pd.DataFrame(moneyness_anchors, columns = ['moneyness'])

	TDF_COLS = ['expiration'] + [f"m{m}" for m in range(80, 125, 5)]
	MDF_COLS = ['moneyness', 'm1', 'm2', 'w1', 'w2', 'iv1', 'iv2']
	SURFACE_COLS = [
		f"m{m1}m{m2}"
		for m2 in [1,3,6,9,12,18,24]
		for m1 in range(80, 125, 5)
	]

	def pre_filters(options):

		options = options[options.zimplied_volatility != 0]
		options = options[options.bid_price != 0]
		return options[options.ask_price != 0]

	def post_filters(options):

		ticker_exp = options.ticker + " " + options.expiration_date
		x = ticker_exp.value_counts()
		x = x[x >= 20]

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

	def calculate_bracket_coords(values, anchors, idx, extra_values=None):

		v1 = values[idx[0]]
		v2 = values[idx[0] + 1]
		v = anchors[idx[1]]

		p1 = 1 / abs(v1 - v)
		p2 = 1 / abs(v2 - v)
		d = p1 + p2

		p1 /= d
		p2 /= d

		coords = [v, v1, v2, p1, p2]

		if extra_values is not None:
			coords.extend([
				extra_values[idx[0]],
				extra_values[idx[0] + 1],
			])

		return coords

	def calculate_brackets(values, anchors, sm, dsm, extra_values=None):

		brackets = [
			calculate_bracket_coords(values, anchors, idx, extra_values)
			for idx in np.argwhere(dsm == 2)
		]

		for idx in np.argwhere(sm == 0):

			bracket = calculate_bracket_coords(values, anchors, idx, extra_values)
			bracket[2] = bracket[1]
			bracket[3:5] = [0.5]*2

			if extra_values is not None:
				bracket[6] = bracket[5]

			brackets.append(bracket)

		return brackets

	def by_ticker(options):

		expirations = options[options.expiration_date.isin(CONFIG['reg_expirations'])]
		expirations = expirations.days_to_expiry.unique()

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

		time_brackets = np.array(sorted(time_brackets))
		expirations = np.unique(time_brackets[:, 1:3].reshape(-1, ))

		ivs = {}
		for expiration in expirations:

			_options = options[options.days_to_expiry == expiration]
			moneyness = _options.moneyness.values
			iv = _options.zimplied_volatility.values

			m, sm, dsm = brackets(moneyness, moneyness_anchors)
			moneyness_brackets = calculate_brackets(
				moneyness,
				moneyness_anchors,
				sm,
				dsm,
				iv
			)

			df = pd.DataFrame(moneyness_brackets, columns = MDF_COLS)
			df['iv'] = df.iv1 * df.w1 + df.iv2 * df.w2
			
			df = df[['moneyness', 'iv']]
			df = df.groupby("moneyness").mean().reset_index()
			df = moneyness_df.merge(df, on='moneyness', how='outer')

			df['expiration'] = expiration
			df = df.fillna(0)
			ivs[expiration] = df.iv.values

		surface = [
			[b[0]] + (ivs[b[1]] * b[3] + ivs[b[2]] * b[4]).tolist()
			for b in time_brackets
		]
		surface = pd.DataFrame(surface, columns = TDF_COLS)
		surface = time_df.merge(surface, on="expiration", how="outer")
		surface = surface.fillna(0).values[:, 1:].reshape(1, -1)
		return pd.DataFrame(surface, columns = SURFACE_COLS)

	options = options[options.days_to_expiry > 0]
	options['mid_price'] = (options.bid_price + options.ask_price) / 2

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
	surface = surface.reset_index(level=0)
	surface['date_current'] = DATE

	return surface

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