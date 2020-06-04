from scipy.stats import norm
from const import CONFIG
import pandas as pd
import numpy as np

###################################################################################################

def calculate_greeks(stock_price, div, options):

	if len(options) == 0:
		return options

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
	options.loc[:, cols] = options[cols].round(6).fillna(0)

	return options.drop(['stock_price', 'dividend_yield', 'rate'], axis=1)