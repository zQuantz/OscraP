from const import DIR, NEW, ENGINE
from procedures import *
from tables import *

import pandas as pd
import numpy as np
import sys, os

sys.path.append("../equities")
from precompute import pre_surface

###################################################################################################

def to_sql(df, tablename, conn):
	df.to_sql(tablename, conn, if_exists="append", index=False, chunksize=100_000)

def derive_surface():

	with ENGINE.connect() as conn:

		print("Initializing Surface")
		conn.execute("DROP TABLE IF EXISTS surfaceBACK;")
		conn.execute(SURFACE_TABLE)

	###############################################################################################

	ohlcs = []
	for folder in os.listdir(NEW['equity']):
		ohlcs.append(pd.read_csv(f"{NEW['equity']}/{folder}/ohlc.csv"))
	
	ohlc = pd.concat(ohlcs)
	ohlc = ohlc[['ticker', 'date_current', 'adjclose_price']]

	surface = []
	for folder in sorted(os.listdir(NEW['equity'])):

		if "options.csv" not in os.listdir(f"{NEW['equity']}/{folder}"):
			continue

		print("Processing", folder)
		options = pd.read_csv(f"{NEW['equity']}/{folder}/options.csv")
		surface.append(pre_surface(options, ohlc, folder))

	print("Indexing IV Surface")
	surface = pd.concat(surface)
	to_sql(surface, "surfaceBACK", ENGINE)

def derive_treasuryratemap():

	with ENGINE.connect() as conn:

		print("Initializing Treasury Rate Maps")
		conn.execute("DROP TABLE IF EXISTS treasuryratemapBACK;")
		conn.execute(TREASURYRATEMAP_TABLE)

	###############################################################################################

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
	to_sql(ratemap, "treasuryratemapBACK", ENGINE)

def derive_stats():

	with ENGINE.connect() as conn:

		print("Initializing Option Stats")
		conn.execute("DROP TABLE IF EXISTS optionstatsBACK;")
		conn.execute(OPTIONSTATS_TABLE)

		print("Initializing OHLC Stats")
		conn.execute("DROP TABLE IF EXISTS ohlcstatsBACK;")
		conn.execute(OHLCSTATS_TABLE)

		print("Initializing Aggregate Option Stats\n")
		conn.execute("DROP TABLE IF EXISTS aggoptionstatsBACK;")
		conn.execute(AGGOPTIONSTATS_TABLE)

	###############################################################################################

	for date in sorted(os.listdir(NEW['equity'])):

		with ENGINE.connect() as conn:

			print(f"Creating dateseries table for date {date}.")
			conn.execute(f"""SET @date_current = "{date}";""")

			for statement in INITDATESERIES.split(";")[:-1]:
				conn.execute(statement)

			print("Inserting OHLC Stats")
			conn.execute(INSERTOHLCSTATS)

			print("Inserting Agg Option Stats")
			conn.execute(INSERTAGGOPTIONSTATS)

			print("Updating Agg Option Stats")
			conn.execute(UPDATEAGGOPTIONSTATS)

			print("Inserting Options Stats")
			conn.execute(INSERTOPTIONSTATS)

			print("\n----------\n")

def derive_tickermaps():

	with ENGINE.connect() as conn:

		print("Initializing Ticker-Date Map")
		conn.execute("DROP TABLE IF EXISTS tickerdatesBACK;")
		conn.execute(TICKERDATES_TABLE)

		print("Initializing Ticker-OptionID Map\n")
		conn.execute("DROP TABLE IF EXISTS tickeroidsBACK;")
		conn.execute(TICKEROIDS_TABLE)

	###############################################################################################

	for date in sorted(os.listdir(NEW['equity'])):

		with ENGINE.connect() as conn:

			print(f"Setting date_current to {date}")
			conn.execute(f"""SET @date_current = "{date}";""")

			print("Inserting Ticker-Dates")
			conn.execute(INSERTTICKERDATES)

			print("Inserting Ticker-Option IDs")
			conn.execute(INSERTTICKEROIDS)

			print("\n----------\n")

def derive():

	derive_surface()
	derive_treasuryratemap()
	derive_stats()
	derive_tickermaps()

if __name__ == '__main__':

	derive()