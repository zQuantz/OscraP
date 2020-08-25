from const import DIR
import tarfile as tar

import pandas as pd
import numpy as np
import sys, os

sys.path.append("../equities")
from precompute import pre_surface

###################################################################################################

OLD = {
	"equity" : f"{DIR}/data/old/equities",
	"rates" : f"{DIR}/data/old/rates",
	"instruments" : f"{DIR}/data/old/instruments",
	"rss" : f"{DIR}/data/old/rss"
}

NEW = {
	key : value.replace("/old/", "/new/")
	for key, value in OLD.items()
}
NEW['rates'] = f"{DIR}/data/new/treasuryrates"
NEW['tickermaps'] = f"{DIR}/data/new/tickermaps"
NEW['treasuryratemap'] = f"{DIR}/data/new/treasuryratemap"

EQUITY_FOLDERS = sorted(os.listdir(OLD['equity']))

###################################################################################################

def options_core():

	def transformation(options):

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
		
		return options

	for folder in EQUITY_FOLDERS:

		print("Options Core Transformation:", folder)

		if "options.csv" not in os.listdir(f"{OLD['equity']}/{folder}/"):
			print("No options file found.")
			continue
		
		options = pd.read_csv(f"{OLD['equity']}/{folder}/options.csv",
							  parse_dates=["date_current", "expiration_date"])

		options = transformation(options)

		new_filename = f"{NEW['equity']}/{folder}/options.csv"
		options.to_csv(new_filename, index=False)

def keystats_core():

	def transformation(keystats):

		return keystats

	for folder in EQUITY_FOLDERS:

		print("Key Stats Core Transformation:", folder)

		if "key_stats.csv" not in os.listdir(f"{OLD['equity']}/{folder}/"):
			print("No Key Stats file found.")
			continue
		
		keystats = pd.read_csv(f"{OLD['equity']}/{folder}/key_stats.csv")
		
		keystats = transformation(keystats)

		new_filename = f"{NEW['equity']}/{folder}/keystats.csv"
		keystats.to_csv(new_filename, index=False)

def analysis_core():

	def transformation(analysis):

		return analysis

	for folder in EQUITY_FOLDERS:

		print("Analysis Core Transformation:", folder)

		if "key_stats.csv" not in os.listdir(f"{OLD['equity']}/{folder}/"):
			print("No analysis file found.")
			continue
		
		analysis = pd.read_csv(f"{OLD['equity']}/{folder}/analysis.csv")

		analysis = transformation(analysis)

		new_filename = f"{NEW['equity']}/{folder}/analysis.csv"
		analysis.to_csv(new_filename, index=False)

def ohlc_core():

	def transformation(ohlc):

		rename = {
			key : f"{key}_price"
			for key in ["open", "high", "low", "close"]
		}
		rename['stock_volume'] = 'volume'
		rename['adj_close'] = 'adjclose_price'
		ohlc = ohlc.rename(rename, axis=1)

		return ohlc

	for folder in EQUITY_FOLDERS:

		print("OHLC Core Transformation:", folder)

		if "ohlc.csv" not in os.listdir(f"{OLD['equity']}/{folder}/"):
			print("No OHLC file found.")
			continue
		
		ohlc = pd.read_csv(f"{OLD['equity']}/{folder}/ohlc.csv")

		ohlc = transformation(ohlc)

		new_filename = f"{NEW['equity']}/{folder}/ohlc.csv"
		ohlc.to_csv(new_filename, index=False)

def rates_core():

	def transformation(rates):

		return rates

	for file in sorted(os.listdir(OLD['rates'])):

		print("Rates Core Transformation:", file)
		
		rates = pd.read_csv(f"{OLD['rates']}/{file}")

		rates = transformation(rates)

		new_filename = f"{NEW['rates']}/{file}"
		
		if rates.shape[1] == 14:
			rates = rates.iloc[:, 1:]
		
		rates.to_csv(new_filename, index=False)

def instruments_core():

	def transformation(instruments):

		return instruments

	for file in sorted(os.listdir(OLD['instruments'])):

		if "_" in file or ".log" in file:
			continue

		print("Instruments Core Transformation:", file)
		
		instruments = pd.read_csv(f"{OLD['instruments']}/{file}")

		transformation(instruments)

		new_filename = f"{NEW['instruments']}/{file}"
		instruments.to_csv(new_filename, index=False)

def rss_core():

	def transformation(rss):

		return rss

	for filename in sorted(os.listdir(OLD['rss'])):

		print("RSS Transformation:", filename)

		with open(f"{OLD['rss']}/{filename}", "r") as file:
			rss = file.read()

		rss = transformation(rss)

		with open(f"{NEW['rss']}/{filename}", "w") as file:
			file.write(rss)

###################################################################################################

def surface():

	ohlcs = []
	for folder in os.listdir(NEW['equity']):
		ohlcs.append(pd.read_csv(f"{NEW['equity']}/{folder}/ohlc.csv"))
	
	ohlc = pd.concat(ohlcs)
	ohlc = ohlc[['ticker', 'date_current', 'adjclose_price']]

	NEW['surface'] = f"{DIR}/data/new/surface"
	os.mkdir(NEW['surface'])

	for folder in sorted(os.listdir(NEW['equity'])):

		if "options.csv" not in os.listdir(f"{NEW['equity']}/{folder}"):
			continue

		options = pd.read_csv(f"{NEW['equity']}/{folder}/options.csv")
		surface_df = pre_surface(options, ohlc, folder)
		
		print("Processing Surface:", folder)

		surface_df.to_csv(f"{NEW['surface']}/{folder}.csv", index=False)

def ticker_maps():

	tickeroids = pd.DataFrame(columns=['ticker', 'option_id'])
	os.mkdir(NEW['tickermaps'])

	for folder in sorted(os.listdir(NEW['equity'])):

		print("Processing Ticker Maps:", folder)

		if "options.csv" not in os.listdir(f"{NEW['equity']}/{folder}/"):
			continue 

		options = pd.read_csv(f"{NEW['equity']}/{folder}/options.csv")

		os.mkdir(f"{NEW['tickermaps']}/{folder}")

		tickerdates = options[['ticker', 'date_current']].drop_duplicates()
		tickerdates.to_csv(f"{NEW['tickermaps']}/{folder}/tickerdates.csv", index=False)

		new_tickeroids = options[['ticker', 'option_id']].drop_duplicates()
		new_tickeroids = new_tickeroids[~new_tickeroids.option_id.isin(tickeroids.option_id)]
		new_tickeroids.to_csv(f"{NEW['tickermaps']}/{folder}/tickeroids.csv", index=False)

		tickeroids = pd.concat([tickeroids, new_tickeroids])

def treasuryratemap():

	rates = []
	for file in sorted(os.listdir(NEW['rates'])):
		ratedf = pd.read_csv(f"{NEW['rates']}/{file}")
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
	ratemap['date_current'] = ratemap.date_current.astype(str)

	os.mkdir(NEW['treasuryratemap'])

	for date in ratemap.date_current.unique():

		print("Processing Treasury Map:", date)

		rmap = ratemap[ratemap.date_current == date]
		rmap.to_csv(f"{NEW['treasuryratemap']}/{date}.csv", index=False)

###################################################################################################

def core():

	options_core()
	ohlc_core()
	analysis_core()
	keystats_core()
	rss_core()
	rates_core()
	instruments_core()

def precalcs():

	treasuryratemap()
	ticker_maps()
	surface()

if __name__ == '__main__':

	core()
	precalcs()
