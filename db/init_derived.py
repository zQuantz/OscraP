from scipy.interpolate import CubicHermiteSpline
from const import DIR, NEW, _connector
from procedures import *
from tables import *

import pandas as pd
import numpy as np
import sys, os

sys.path.append("../equities")
from calculations import synth_surface

def derive_surface():

	print("Initializing Surface")
	_connector.execute("DROP TABLE IF EXISTS surfaceBACK;")
	_connector.execute(SURFACE_TABLE)

	ohlcs = []
	for folder in NEW['equity'].iterdir():
		ohlcs.append(pd.read_csv(folder / "ohlc.csv"))
	ohlc = pd.concat(ohlcs)
	ohlc = ohlc[['ticker', 'date_current', 'adjclose_price']]

	surfaces = []
	for folder in sorted(NEW['equity'].iterdir()):

		file = folder / "options.csv"
		if not file.exists():
			continue

		print("Processing Surface", folder.name)
		options = pd.read_csv(file)
		surfaces.append(synth_surface(options, ohlc, folder.name))
		print(surfaces[-1])
	
	print("Indexing IV Surface")
	surfaces = pd.concat(surfaces)
	_connector.write("surfaceBACK", surfaces)

def derive_treasuryratemap():

	print("Initializing Treasury Rate Maps")
	_connector.execute("DROP TABLE IF EXISTS treasuryratemapBACK;")
	_connector.execute(TREASURYRATEMAP_TABLE)

	rates = [
		pd.read_csv(file)
		for file in sorted(NEW['treasuryrates'].iterdir())
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

		chs = CubicHermiteSpline(t_map, r_map, [0]*len(t_map))

		rm_df = pd.DataFrame()
		rm_df['days_to_expiry'] = np.arange(0, 365 * 10 + 1).astype(int)
		rm_df['rate'] = chs(rm_df.days_to_expiry.values)

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

	print("Initializing Surface Skew")
	_connector.execute("DROP TABLE IF EXISTS surfaceskewBACK;")
	_connector.execute(SURFACESKEW_TABLE)

	print("Initializing Surface Stats")
	_connector.execute("DROP TABLE IF EXISTS surfacestatsBACK;")
	_connector.execute(SURFACESTATS_TABLE)

	print("Initializing OHLC Stats")
	_connector.execute("DROP TABLE IF EXISTS ohlcstatsBACK;")
	_connector.execute(OHLCSTATS_TABLE)

	print("Initializing Aggregate Option Stats")
	_connector.execute("DROP TABLE IF EXISTS aggoptionstatsBACK;")
	_connector.execute(AGGOPTIONSTATS_TABLE)

	print("Initializing Option Counts")
	_connector.execute("DROP TABLE IF EXISTS optionscountsBACK;")
	_connector.execute(OPTIONCOUNTS_TABLE)

	print("Initializing Analysis Counts")
	_connector.execute("DROP TABLE IF EXISTS analysiscountsBACK;")
	_connector.execute(ANALYSISCOUNTS_TABLE)

	print("Initializing Keystats Counts")
	_connector.execute("DROP TABLE IF EXISTS keystatscountsBACK;")
	_connector.execute(KEYSTATSCOUNTS_TABLE)

	###############################################################################################

	SUBSET = """
		WHERE
			ticker IN
			(
				SELECT
					ticker
				FROM
					tickerdatesBACK
				WHERE
					date_current = "{date}"
				AND ASCII(SUBSTRING(ticker, 1, 1))
					BETWEEN {n1} AND {n2}
			)
	"""

	ranges = [
		(65, 69),
		(70, 79),
		(80, 91)
	]

	###############################################################################################

	for date in sorted(os.listdir(NEW['equity'])):

		print(f"Creating dateseries table for date {date}.")
		_connector.date = date
		_connector.init_date_series("BACK")

		print("Inserting OHLC Stats")
		_connector.execute(INSERT_OHLC_STATS.format(modifier="BACK", subset="", date=date))

		print("Inserting Agg Option Stats")
		_connector.execute(INSERT_AGG_OPTION_STATS.format(modifier="BACK", subset="", date=date))

		print("Updating Agg Option Stats")
		_connector.execute(UPDATE_AGG_OPTION_STATS.format(modifier="BACK", subset="", date=date))

		print("Inserting Options Stats")
		for i, _range in enumerate(ranges):
			print(f"Batch #{i}. {_range}")
			subset = SUBSET.format(date=date, n1=_range[0], n2=_range[1])
			_connector.execute(INSERT_OPTION_STATS.format(modifier="BACK", subset=subset, date=date))

		print("Inserting Surface Skew")
		_connector.execute(INSERT_SURFACE_SKEW.format(modifier="BACK", subset="", date=date))

		print("Inserting Surface Stats")
		_connector.execute(INSERT_SURFACE_STATS.format(modifier="BACK", subset="", date=date))

		print("Inserting Options Counts")
		_connector.execute(INSERT_OPTION_COUNTS.format(modifier="BACK", subset="", date=date))

		print("Inserting Analysis Counts")
		_connector.execute(INSERT_ANALYSIS_COUNTS.format(modifier="BACK", subset="", date=date))

		print("Inserting Keystats Counts")
		_connector.execute(INSERT_KEYSTATS_COUNTS.format(modifier="BACK", subset="", date=date))

		print("\n----------\n")

def derive_tickermaps():

	print("Initializing Ticker-Date Map")
	_connector.execute("DROP TABLE IF EXISTS tickerdatesBACK;")
	_connector.execute(TICKERDATES_TABLE)

	print("Initializing Ticker-OptionID Map\n")
	_connector.execute("DROP TABLE IF EXISTS tickeroidsBACK;")
	_connector.execute(TICKEROIDS_TABLE)

	for date in sorted(os.listdir(NEW['equity'])):

		print("Processing", date)

		print("Inserting Ticker-Dates")
		_connector.execute(INSERT_TICKER_DATES.format(modifier="BACK", subset="", date=date))

		print("Inserting Ticker-Option IDs")
		_connector.execute(INSERT_TICKER_OIDS.format(modifier="BACK", subset="", date=date))

		print("\n----------\n")

def derive():

	derive_treasuryratemap()
	derive_tickermaps()
	derive_surface()
	derive_stats()

if __name__ == '__main__':

	derive()