from const import DIR, NEW, _connector
from procedures import *
from tables import *

import pandas as pd
import numpy as np
import sys, os

sys.path.append("../equities")
from calculations import surface

def derive_surface():

	print("Initializing Surface")
	_connector.execute("DROP TABLE IF EXISTS surfaceBACK;")
	_connector.execute(SURFACE_TABLE)

	ohlcs = []
	for folder in NEW['equity'].iterdir():
		ohlcs.append(pd.read_csv(folder / "ohlc.csv"))
	ohlc = pd.concat(ohlcs)
	ohlc = ohlc[['ticker', 'date_current', 'adjclose_price']]

	surface = []
	for folder in sorted(NEW['equity'].iterdir()):

		file = folder / "options.csv"
		if not file.exists():
			continue

		print("Processing Surface", folder.name)
		options = pd.read_csv(file)
		surface.append(pre_surface(options, ohlc, folder.name))

	print("Indexing IV Surface")
	surface = pd.concat(surface)
	_connector.write("surfaceBACK", surface)

def derive_treasuryratemap():

	print("Initializing Treasury Rate Maps")
	_connector.execute("DROP TABLE IF EXISTS treasuryratemapBACK;")
	_connector.execute(TREASURYRATEMAP_TABLE)

	rates = [
		pd.read_csv(file)
		for file in sorted(NEW['rates'].iterdir())
	]

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
		t_map = np.array(t_map)

		r_map = df.iloc[-1, 1:].values
		r_map = np.array([0] + r_map.tolist())
		r_map /= 100

		def get_rate(t):
			
			if t >= (30 * 360):
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
	
	print("Indexing Treasury Rate Map")
	_connector.write("treasuryratemapBACK", ratemap)

def derive_stats():

	print("Initializing Option Stats")
	_connector.execute("DROP TABLE IF EXISTS optionstatsBACK;")
	_connector.execute(OPTIONSTATS_TABLE)

	print("Initializing OHLC Stats")
	_connector.execute("DROP TABLE IF EXISTS ohlcstatsBACK;")
	_connector.execute(OHLCSTATS_TABLE)

	print("Initializing Aggregate Option Stats")
	_connector.execute("DROP TABLE IF EXISTS aggoptionstatsBACK;")
	_connector.execute(AGGOPTIONSTATS_TABLE)

	print("Initializing Option Counts")
	_connector.execute("DROP TABLE IF EXISTS optioncountsBACK;")
	_connector.execute(OPTIONCOUNTS_TABLE)

	print("Initializing Analysis Counts")
	_connector.execute("DROP TABLE IF EXISTS analysiscountsBACK;")
	_connector.execute(ANALYSISCOUNTS_TABLE)

	print("Initializing Keystats Counts")
	_connector.execute("DROP TABLE IF EXISTS keystatscountsBACK;")
	_connector.execute(KEYSTATSCOUNTS_TABLE)

	for date in sorted(os.listdir(NEW['equity'])):

		print(f"Creating dateseries table for date {date}.")
		_connector.execute(f"""SET @date_current = "{date}";""")

		for statement in INIT_DATE_SERIES:
			_connector.execute(statement.format(modifier="BACK", subset=""))

		print("Inserting OHLC Stats")
		_connector.execute(INSERT_OHLC_STATS.format(modifier="BACK", subset=""))

		print("Inserting Agg Option Stats")
		_connector.execute(INSERT_AGG_OPTION_STATS.format(modifier="BACK", subset=""))

		print("Updating Agg Option Stats")
		_connector.execute(UPDATE_AGG_OPTION_STATS.format(modifier="BACK", subset=""))

		print("Inserting Options Stats")
		_connector.execute(INSERT_OPTION_STATS.format(modifier="BACK", subset=""))

		print("Inserting Options Counts")
		_connector.execute(INSERT_OPTION_COUNTS.format(modifier="BACK", subset=""))

		print("Inserting Analysis Counts")
		_connector.execute(INSERT_ANALYSIS_COUNTS.format(modifier="BACK", subset=""))

		print("Inserting Keystats Counts")
		_connector.execute(INSERT_KEYSTATS_COUNTS.format(modifier="BACK", subset=""))

		print("\n----------\n")

def derive_tickermaps():

	print("Initializing Ticker-Date Map")
	_connector.execute("DROP TABLE IF EXISTS tickerdatesBACK;")
	_connector.execute(TICKERDATES_TABLE)

	print("Initializing Ticker-OptionID Map\n")
	_connector.execute("DROP TABLE IF EXISTS tickeroidsBACK;")
	_connector.execute(TICKEROIDS_TABLE)

	for date in sorted(os.listdir(NEW['equity'])):

		print(f"Setting date_current to {date}")
		_connector.execute(f"""SET @date_current = "{date}";""")

		print("Inserting Ticker-Dates")
		_connector.execute(INSERT_TICKER_DATES.format(modifier="BACK", subset=""))

		print("Inserting Ticker-Option IDs")
		_connector.execute(INSERT_TICKER_OIDS.format(modifier="BACK", subset=""))

		print("\n----------\n")

def derive():

	# derive_tickermaps()
	# derive_treasuryratemap()
	# derive_surface()
	# derive_stats()

if __name__ == '__main__':

	derive()