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
	greeks = greeks.round(5)
	greeks = greeks.fillna(0)

	options = pd.concat([options, greeks], axis=1)
	options = options.sort_values(["time_to_expiry", "option_type", "strike_price"],
								  ascending=[False, True, True])

	CONFIG['rates']['rates'] = rates

	return options