from const import DIR

import pandas as pd
import numpy as np
import sys, os

sys.path.append("../equities")
from precompute import pre_surface

###################################################################################################

equity_dir = f"{DIR}/data/old/equities"
rates_dir = f"{DIR}/data/old/rates"
instruments_dir = f"{DIR}/data/old/instruments"
EQUITY_FOLDERS = sorted(os.listdir(equity_dir))

###################################################################################################

def options_core():

	for folder in EQUITY_FOLDERS:

		print("Options Core Transformation:", folder)

		if "options.csv" not in os.listdir(f"{equity_dir}/{folder}/"):
			print("No options file found.")
			continue
		
		options = pd.read_csv(f"{equity_dir}/{folder}/options.csv",
							  parse_dates=["date_current", "expiration_date"])
		
		cols = ["time_to_expiry", "delta", "gamma", "theta", "vega", "rho"]
		options = options.drop(cols, axis=1)

		days = options.expiration_date - options.date_current
		days = days.dt.days
		options['days_to_expiry'] = days

		renames = {
				"bid" : "bid_price",
				"ask" : "ask_price"
			}
		options = options.rename(renames, axis=1)

		new_filename = f"{equity_dir}/{folder}/options.csv"
		new_filename = new_filename.replace("/old/", "/new/")
		options.to_csv(new_filename, index=False)

def keystats_core():

	for folder in EQUITY_FOLDERS:

		print("Key Stats Core Transformation:", folder)

		if "key_stats.csv" not in os.listdir(f"{equity_dir}/{folder}/"):
			print("No Key Stats file found.")
			continue
		
		key_stats = pd.read_csv(f"{equity_dir}/{folder}/key_stats.csv")
		new_filename = f"{equity_dir}/{folder}/key_stats.csv"
		new_filename = new_filename.replace("/old/", "/new/")
		key_stats.to_csv(new_filename, index=False)

def analysis_core():

	for folder in EQUITY_FOLDERS:

		print("Analysis Core Transformation:", folder)

		if "key_stats.csv" not in os.listdir(f"{equity_dir}/{folder}/"):
			print("No analysis file found.")
			continue
		
		analysis = pd.read_csv(f"{equity_dir}/{folder}/analysis.csv")
		new_filename = f"{equity_dir}/{folder}/analysis.csv"
		new_filename = new_filename.replace("/old/", "/new/")
		analysis.to_csv(new_filename, index=False)

def ohlc_core():

	for folder in EQUITY_FOLDERS:

		print("OHLC Core Transformation:", folder)

		if "ohlc.csv" not in os.listdir(f"{equity_dir}/{folder}/"):
			print("No OHLC file found.")
			continue
		
		ohlc = pd.read_csv(f"{equity_dir}/{folder}/ohlc.csv")
		new_filename = f"{equity_dir}/{folder}/ohlc.csv"
		new_filename = new_filename.replace("/old/", "/new/")
		ohlc.to_csv(new_filename, index=False)

def rates_core():

	for file in sorted(os.listdir(rates_dir)):

		print("Rates Core Transformation:", file)
		
		rates = pd.read_csv(f"{rates_dir}/{file}")
		new_filename = f"{rates_dir}/{file}"
		new_filename = new_filename.replace("/old/", "/new/")
		
		if rates.shape[1] == 14:
			rates = rates.iloc[:, 1:]
		
		rates.to_csv(new_filename, index=False)

def instruments_core():

	for file in sorted(os.listdir(instruments_dir)):

		if "_" in file or ".log" in file:
			continue

		print("Instruments Core Transformation:", file)
		
		instruments = pd.read_csv(f"{instruments_dir}/{file}")
		new_filename = f"{instruments_dir}/{file}"
		new_filename = new_filename.replace("/old/", "/new/")
		instruments.to_csv(new_filename, index=False)

def surface():

	new_equity_dir = equity_dir.replace("/old/", "/new/")

	ohlcs = []
	for folder in os.listdir(new_equity_dir):
		ohlcs.append(pd.read_csv(f"{new_equity_dir}/{folder}/ohlc.csv"))
	
	ohlc = pd.concat(ohlcs)
	ohlc = ohlc[['ticker', 'date_current', 'adj_close']]

	for folder in sorted(os.listdir(new_equity_dir)):

		print("Processing Surface:", folder)

		options = pd.read_csv(f"{new_equity_dir}/{folder}/options.csv")
		surface_df, timesurface_df = pre_surface(options, ohlc, folder)
		
		print(len(surface_df), len(timesurface_df))

		surface_df.to_csv(f"{new_equity_dir}/{folder}/surface.csv", index=False)
		timesurface_df.to_csv(f"{new_equity_dir}/{folder}/timesurface.csv", index=False)

def ticker_maps():

	new_equity_dir = equity_dir.replace("/old/", "/new/")
	tickeroids = pd.DataFrame(columns=['ticker', 'option_id'])

	for folder in sorted(os.listdir(new_equity_dir)):

		print("Processing Ticker Maps:", folder)

		if "options.csv" not in os.listdir(f"{new_equity_dir}/{folder}/"):
			continue 

		options = pd.read_csv(f"{new_equity_dir}/{folder}/options.csv")

		tickerdates = options[['ticker', 'date_current']].drop_duplicates()
		tickerdates.to_csv(f"{new_equity_dir}/{folder}/tickerdates.csv", index=False)

		new_tickeroids = options[['ticker', 'option_id']].drop_duplicates()
		new_tickeroids = new_tickeroids[~new_tickeroids.option_id.isin(tickeroids.option_id)]
		new_tickeroids.to_csv(f"{new_equity_dir}/{folder}/tickeroids.csv", index=False)

		tickeroids = pd.concat([tickeroids, new_tickeroids])

def treasuryratemap():

	new_rate_dir = rates_dir.replace("/old/", "/new/")
	rates = []

	for file in sorted(os.listdir(new_rate_dir)):
		ratedf = pd.read_csv(f"{new_rate_dir}/{file}")
		rates.append(ratedf)

	rates = pd.concat(rates)
	rates['date_current'] = pd.to_datetime(rates.date_current)
	rates = rates[rates.date_current >= "2019-01-01"]

	def by_date(df):

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
		t_map = np.array(t_map) / 360

		r_map = df.iloc[-1, 1:].values
		r_map = np.array([0] + r_map.tolist())
		r_map /= 100

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

		rm_df = pd.DataFrame()
		rm_df['days_to_expiry'] = np.arange(0, 365 * 10 + 1).astype(int)
		rm_df['rate'] = rm_df.days_to_expiry.apply(get_rate)

		return rm_df

	ratemap = rates.iloc[:, :].groupby("date_current").apply(by_date)
	ratemap = ratemap.reset_index()
	ratemap = ratemap[['date_current', 'days_to_expiry', 'rate']]

	new_ratemap_dir = f"{DIR}/data/new/ratemap"
	os.mkdir(new_ratemap_dir)

	for date in ratemap.date_current.unique():

		print("Processing Treasury Map:", date)

		rmap = ratemap[ratemap.date_current == date]
		rmap.to_csv(f"{new_ratemap_dir}/{date}.csv", index=False)

def core():

	options_core()
	ohlc_core()
	analysis_core()
	keystats_core()

	rates_core()
	instruments_core()

def precalcs():

	surface()
	ticker_maps()
	treasuryratemap()

if __name__ == '__main__':

	core()
	precalcs()
