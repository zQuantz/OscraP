from scipy.interpolate import CubicHermiteSpline
from joblib import Parallel, delayed

from scipy.optimize import brentq
from scipy.stats import norm
from pathlib import Path
import pandas as pd
import numpy as np
import sys, os

###################################################################################################

BASE = Path("data/new")
EQUITIES = BASE / "equities"
RATES = BASE / "treasuryrates"

SUBSET = [
	"2020-12-30",
	"2020-12-31"
]

###################################################################################################

def calculate_ratemap():

	t_map = [
		0,
		30,
		60,
		90,
		180,
		12 * 30,
		24 * 30,
		36 * 30,
		60 * 30,
		72 * 30,
		120 * 30,
		240 * 30,
		360 * 30
	]
	t_map = np.array(t_map)

	rates = [
		pd.read_csv(file)
		for file in sorted(RATES.iterdir())
	]
	rates = pd.concat(rates)

	def by_date(df):

		r_map = df.iloc[0, 1:].values
		r_map = [0] + r_map.tolist()

		chs = CubicHermiteSpline(t_map, r_map, [0]*len(r_map))
		ratemap = pd.DataFrame()
		ratemap['days_to_expiry'] = np.arange(0, 365 * 10 + 1).astype(int)
		ratemap['rate'] = chs(ratemap.days_to_expiry.values)

		return ratemap

	return rates.groupby("date_current").apply(by_date).reset_index(level=0)

def get_ohlc():

	ohlc = [
		pd.read_csv(file / "ohlc.csv")
		for file in sorted(EQUITIES.iterdir())
	]
	return pd.concat(ohlc).reset_index(drop=True)

def bs_price(v, S, K, T, r, q, t, M):
	
	d1 = np.log(S / K) + (r + 0.5 * v * v) * T
	d1 /= np.sqrt(T) * v
	d2 = d1 - np.sqrt(T) * v

	return t * (S * np.exp(-q * T) * norm.cdf(t * d1) - K * np.exp(-r * T) * norm.cdf(t * d2))

def root(v, *args):
	return bs_price(v, *args) - args[-1]

def finder(dates, ohlc, ratemap, job_id):

	for i, date in enumerate(sorted(dates)[::-1]):

		if SUBSET and date not in SUBSET:
			continue

		options = pd.read_csv(EQUITIES / date / "options.csv")
		l1 = len(options)

		options = options.merge(ohlc, on=['date_current', 'ticker'], how="inner")
		l2 = len(options)

		options = options.merge(ratemap, on=['date_current', 'days_to_expiry'], how='inner')
		l3 = len(options)

		print(f"Job #{job_id} @ {date}. {i / len(dates) * 100}%", l1, l2, l3)

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
		print((options.implied_volatility - options.zimplied_volatility).describe())

		na_roots = options[options.zimplied_volatility == 0].shape[0]
		print("Percentage of NA Roots:", na_roots / len(options))
		print("*** ___ *** ___ *** ___ *** ___ *** ___ ***")

		cols = ["stock_price", "dividend_yield", "rate"]
		options = options.drop(cols, axis=1) 
		options.to_csv(EQUITIES / date / "options.csv")

if __name__ == "__main__":

	ratemap = calculate_ratemap()

	cols = ['date_current', 'ticker', 'adjclose_price', 'dividend_yield']
	ohlc = get_ohlc()[cols]
	ohlc = ohlc.rename({"adjclose_price" : "stock_price"}, axis=1)

	dates = [
		file.name
		for file in sorted(EQUITIES.iterdir())
	]

	cs = 35
	chunks = [
		dates[i - cs : i]
		for i in range(cs, len(dates) + cs, cs)
	]

	Parallel(n_jobs=2)(
		delayed(finder)(chunk, ohlc, ratemap, i)
		for i, chunk in enumerate(chunks)
	)
