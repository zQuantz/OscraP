from scipy.stats import norm
from const import CONFIG
import pandas as pd
import numpy as np

###################################################################################################

def calculate_greeks(stock_price, div, options):

	t_map = CONFIG['rates']['t_map']
	r_map = CONFIG['rates']['r_map']
	rates = CONFIG['rates']['rates']

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

	time_to_expirations = options.time_to_expiry.unique()	
	unique_rates = {
		tte : get_rate(tte)
		for tte in time_to_expirations
	}

	options['rate'] = options.time_to_expiry.map(unique_rates)
	options['stock_price'] = stock_price
	options['dividend_yield'] = div

	###############################################################################################

	o = options.copy()
	m = o.option_type.map({"C" : 1, "P" : -1})

	eqt = np.exp(-o.dividend_yield * o.time_to_expiry)
	kert = o.strike_price * np.exp(-o.rate * o.time_to_expiry)

	d1 = np.log(o.stock_price / o.strike_price)
	d1 += (o.rate - o.dividend_yield + 0.5 * (o.implied_volatility ** 2)) * o.time_to_expiry
	d1 /= o.implied_volatility * np.sqrt(o.time_to_expiry)
	d2 = d1 - o.implied_volatility * np.sqrt(o.time_to_expiry)

	delta = m * eqt * norm.cdf(m * d1)

	gamma = eqt * norm.pdf(d1)
	gamma /= (o.stock_price * o.implied_volatility * np.sqrt(o.time_to_expiry))

	vega = o.stock_price * eqt * norm.pdf(d1) * np.sqrt(o.time_to_expiry)	

	rho = m * o.time_to_expiry * kert * norm.cdf(m * d2)

	theta = (o.stock_price * norm.pdf(m * d1) * o.implied_volatility)
	theta *= -eqt / (2 * np.sqrt(o.time_to_expiry))
	theta -= m * o.rate * kert * norm.cdf(m * d2)
	theta += m * o.dividend_yield * o.stock_price * eqt * norm.cdf(m * d1)

	###############################################################################################

	options['delta'] = delta
	options['gamma'] = gamma
	options['theta'] = theta / 365
	options['vega'] = vega / 100
	options['rho'] = rho / 100

	cols = ['delta', 'gamma', 'theta', 'vega', 'rho']
	options.loc[:, cols] = options[cols].replace([-np.inf, np.inf], np.nan)
	options.loc[:, cols] = options[cols].fillna(0)

	return options.drop(['stock_price', 'dividend_yield', 'rate'], axis=1)

def calculate_greeks2(stock_price, div, options):

	t_map = CONFIG['rates']['t_map']
	r_map = CONFIG['rates']['r_map']
	rates = CONFIG['rates']['rates']

	def get_rate(t):
	
		if t in rates:
			return rates[t]

		if t >= 30:
			rates[t] = r_map[-1]
			return r_map[-1]
		
		b1 = t_map <= t
		b2 = t_map > t

		r1 = r_map[b1][-1]
		r2 = r_map[b2][0]

		t1 = t_map[b1][-1]
		t2 = t_map[b2][0]
		
		interpolated_rate = (t - t1) / (t2 - t1)
		interpolated_rate *= (r2 - r1)

		rates[t] = interpolated_rate + r1
		return rates[t]

	greeks = []

	s, q = stock_price, div
	ttes = options.time_to_expiry.values
	otypes = options.option_type.values
	strikes = options.strike_price.values
	sigs = options.implied_volatility.values

	for t, type_, k, sig in zip(ttes, otypes, strikes, sigs):
		
		r = get_rate(t)
		
		eqt = np.exp(-q * t)
		kert = k * np.exp(-r * t)
			
		d1 = np.log(s / k) + (r - q + 0.5 * (sig*sig)) * t
		d1 /= sig * np.sqrt(t)
		d2 = d1 - sig * np.sqrt(t)
		
		m = (1, d1) if type_ == "C" else (-1,-d1)
		delta = m[0] * eqt * norm.cdf(m[1])
		
		gamma = eqt * norm.pdf(d1)
		gamma /= s * sig * np.sqrt(t)

		m = (1, d1, d2) if type_ == "C" else (-1, -d1, -d2)
		theta = -eqt * (s * norm.pdf(m[1]) * sig) / (2 * np.sqrt(t))
		theta -= m[0] * r * kert * norm.cdf(m[2])
		theta += m[0] * q * s * eqt * norm.cdf(m[1])
		theta *= 1 * m[0]
		
		vega = s * eqt * norm.pdf(d1) * np.sqrt(t)
		
		m = (1,d2) if type_ == "C" else (-1,-d2)
		rho = m[0] * t * kert * norm.cdf(m[1])
		
		greeks.append([
			delta,
			gamma,
			vega / 100,
			rho / 100,
			theta / 365
		])

	greeks = pd.DataFrame(greeks, options.index, columns = ['delta', 'gamma', 'vega', 'rho', 'theta'])
	greeks = greeks.fillna(0)

	options = pd.concat([options, greeks], axis=1)
	options = options.sort_values(["time_to_expiry", "option_type", "strike_price"],
								  ascending=[False, True, True])

	CONFIG['rates']['rates'] = rates

	return options