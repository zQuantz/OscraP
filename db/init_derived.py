from scipy.interpolate import CubicHermiteSpline
from const import DIR, NEW, _connector
from joblib import delayed, Parallel
from pathlib import Path
from procedures import *
from tables import *

import pandas as pd
import numpy as np
import sys, os

sys.path.append("../equities")
from calculations import calculate_surface, calculate_regular_expiries

SUBSET = None

###################################################################################################

def derive_treasuryratemap():

	# if not SUBSET:

	# 	print("Initializing Treasury Rate Maps")
	# 	_connector.execute("DROP TABLE IF EXISTS treasuryratemapBACK;")
	# 	_connector.execute(TREASURYRATEMAP_TABLE)

	rates = []
	for file in sorted(NEW['treasuryrates'].iterdir()):

		print("Processing Ratemap:", file.name)

		if SUBSET and file.name.split(".")[0] not in SUBSET:
			continue

		rates.append(pd.read_csv(file))

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
	print(ratemap)
	# _connector.write("treasuryratemapBACK", ratemap)

	return ratemap

def derive_surface(ratemap):

	if not SUBSET:

		print("Initializing Surface")
		_connector.execute("DROP TABLE IF EXISTS surfaceBACK;")
		_connector.execute(SURFACE_TABLE)

		print("Initializing zSurface")
		_connector.execute("DROP TABLE IF EXISTS zsurfaceBACK;")
		_connector.execute(SURFACE_TABLE.replace("surfaceBACK", "zsurfaceBACK"))

	ohlcs = []
	for folder in NEW['equity'].iterdir():

		if SUBSET and folder.name not in SUBSET:
			continue

		ohlcs.append(pd.read_csv(folder / "ohlc.csv"))
	
	ohlc = pd.concat(ohlcs)
	ohlc = ohlc[['ticker', 'date_current', 'adjclose_price']]
	ohlc = ohlc.rename({"adjclose_price" : "stock_price"}, axis=1)

	def calculate_and_index(job_id, folders, ohlc, ratemap):

		for folder in folders:

			if SUBSET and folder.name not in SUBSET:
				continue

			file = folder / "options.csv"
			if not file.exists():
				continue

			min_date = folder.name
			max_date = f"{int(min_date[:4])+10}"+min_date[4:]
			reg_expirations = calculate_regular_expiries(min_date, max_date)

			print("Processing Surface", folder.name)

			options = pd.read_csv(file)
			options = options.merge(ohlc, on=['ticker', 'date_current'], how='inner')
			options = options.merge(ratemap, on=['date_current', 'days_to_expiry'], how='inner')

			zsurface, surface = calculate_surface(options, reg_expirations)
			zsurface['date_current'] = folder.name
			surface['date_current'] = folder.name

			print(zsurface.ticker.nunique(), options.ticker.nunique())
			print(zsurface)

			print(surface.ticker.nunique(), options.ticker.nunique())
			print(surface)

			zsurface.to_csv(f"zsurfaces/{folder.name}.csv", index=False)
			surface.to_csv(f"surfaces/{folder.name}.csv", index=False)

			print("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")

	CS = 50
	folders = sorted(NEW['equity'].iterdir())
	
	chunks = [
		folders[i - CS : i]
		for i in range(CS, len(folders) + CS, CS)
	]

	Parallel(n_jobs = 8)(
		delayed(calculate_and_index)(job_id, chunk, ohlc, ratemap)
		for job_id, chunk in enumerate(chunks)
	)

	zsurfaces, surfaces = [], []
	zpath, path = Path("zsurfaces/"), Path("surfaces/")
	for zfile, file in zip(sorted(zpath.iterdir()), sorted(path.iterdir())):

		print(zfile.name, file.name)
		
		zsurfaces.append(pd.read_csv(zfile))
		surfaces.append(pd.read_csv(file))

		if len(surfaces) % 20 == 0:
		
			print("Indexing zSurfaces.")
			zsurfaces = pd.concat(zsurfaces).reset_index(drop=True)
			_connector.write("zsurfaceBACK", zsurfaces)
			zsurfaces = []

			print("Indexing Surfaces.")
			surfaces = pd.concat(surfaces).reset_index(drop=True)
			_connector.write("surfaceBACK", surfaces)
			surfaces = []

	if len(surfaces) != 0:

		print("Final zSurface Index.")
		zsurfaces = pd.concat(zsurfaces).reset_index(drop=True)
		_connector.write("zsurfaceBACK", zsurfaces)

		print("Final Surface Index.")
		surfaces = pd.concat(surfaces).reset_index(drop=True)
		_connector.write("surfaceBACK", surfaces)

def derive_stats():

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

	for date in sorted(os.listdir(NEW['equity'])):

		print("Processing date:", date)

		print("Inserting Agg Option Stats")
		_connector.execute(INSERT_AGG_OPTION_STATS.format(modifier="BACK", subset="", date=date))

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

	ratemap = derive_treasuryratemap()
	derive_surface(ratemap)
	
	derive_tickermaps()
	derive_stats()

if __name__ == '__main__':

	derive()