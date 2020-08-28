from const import CONFIG, DIR
import sqlalchemy as sql
from tables import *
import pandas as pd
import sys, os

###################################################################################################

NEW = f"{DIR}/data/new"
db = CONFIG['db_address'].replace("test", "finance")
engine = sql.create_engine(db)

###################################################################################################

def to_sql(df, tablename, conn):
	df.to_sql(tablename, conn, if_exists="append", index=False, chunksize=100_000)

def index_instruments():

	print("Instruments")

	instruments = []
	for file in sorted(os.listdir(f"{NEW}/instruments")):
		instruments.append(pd.read_csv(f"{NEW}/instruments/{file}", parse_dates=['last_updated']))
	
	instruments = pd.concat(instruments)
	instruments = instruments.sort_values(["last_updated", "market_cap"], ascending=[False, False])
	instruments = instruments.drop_duplicates(subset=["ticker", "exchange_code"], keep="first")

	print("Indexing Instruments")

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS instrumentsBACK;")
	conn.execute(INSTRUMENT_TABLE)
	to_sql(instruments, "instrumentsBACK", conn)
	conn.close()

def index_rates():

	print("Treasury Rates")

	treasuryrates = []
	for file in sorted(os.listdir(f"{NEW}/treasuryrates")):
		treasuryrates.append(pd.read_csv(f"{NEW}/treasuryrates/{file}", parse_dates=["date_current"]))
	
	treasuryrates = pd.concat(treasuryrates)
	treasuryrates = treasuryrates[treasuryrates.date_current >= "2019-01-01"]
	treasuryrates = treasuryrates.sort_values("date_current", ascending=True)

	print("Indexing Treasury Rates")
	print(treasuryrates)

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS treasuryratesBACK;")
	conn.execute(TREASURYRATES_TABLE)
	to_sql(treasuryrates, "treasuryratesBACK", conn)
	conn.close()

	###############################################################################################

	print("Treasury Ratemap")

	treasuryratemap = []
	for file in sorted(os.listdir(f"{NEW}/treasuryratemap")):
		treasuryratemap.append(pd.read_csv(f"{NEW}/treasuryratemap/{file}", parse_dates=["date_current"]))
	
	treasuryratemap = pd.concat(treasuryratemap)
	treasuryratemap = treasuryratemap[treasuryratemap.date_current >= "2019-01-01"]
	treasuryratemap = treasuryratemap.sort_values(["date_current", "days_to_expiry"])

	print("Indexing Treasury Ratemap")
	print(treasuryratemap)

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS treasuryratemapBACK;")
	conn.execute(TREASURYRATEMAP_TABLE)
	to_sql(treasuryratemap, "treasuryratemapBACK", conn)
	conn.close()

	###############################################################################################

def index_tickermaps():

	print("Ticker Maps")

	tickeroids, tickerdates = [], []
	for folder in sorted(os.listdir(f"{NEW}/tickermaps")):
		tickeroids.append(pd.read_csv(f"{NEW}/tickermaps/{folder}/tickeroids.csv"))
		tickerdates.append(pd.read_csv(f"{NEW}/tickermaps/{folder}/tickerdates.csv"))

	tickeroids = pd.concat(tickeroids)
	tickerdates = pd.concat(tickerdates)

	###############################################################################################

	print("Ticker Order IDs")
	print(tickeroids)

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS tickeroidsBACK;")
	conn.execute(TICKEROIDS_TABLE)
	to_sql(tickeroids, "tickeroidsBACK", conn)
	conn.close()

	print("Ticker Date Map")
	print(tickerdates)

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS tickerdatesBACK;")
	conn.execute(TICKERDATES_TABLE)
	to_sql(tickerdates, "tickerdatesBACK", conn)
	conn.close()

def index_equities():

	print("Equities Excl. Options")

	analysis, ohlc, keystats = [], [], []
	for folder in os.listdir(f"{NEW}/equities"):

		files = os.listdir(f"{NEW}/equities/{folder}")

		if "ohlc.csv" in files:
			ohlc.append(pd.read_csv(f"{NEW}/equities/{folder}/ohlc.csv"))
		
		if "analysis.csv" in files:
			analysis.append(pd.read_csv(f"{NEW}/equities/{folder}/analysis.csv"))
		
		if "keystats.csv" in files:
			keystats.append(pd.read_csv(f"{NEW}/equities/{folder}/keystats.csv"))

	ohlc = pd.concat(ohlc)
	analysis = pd.concat(analysis)
	keystats = pd.concat(keystats)

	###############################################################################################

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS ohlcBACK;")
	conn.execute("DROP TABLE IF EXISTS analysisBACK;")
	conn.execute("DROP TABLE IF EXISTS keystatsBACK;")
	conn.execute(OHLC_TABLE)
	conn.execute(ANALYSIS_TABLE)
	conn.execute(KEYSTATS_TABLE)
	conn.close()

	print("OHLC")
	print(ohlc)
	to_sql(ohlc, "ohlcBACK", engine)

	print("Analysis")
	print(analysis)
	to_sql(analysis, "analysisBACK", engine)

	print("Key Stats")
	print(keystats)
	to_sql(keystats, "keystatsBACK", engine)

def index_options():

	print("Options")

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS optionsBACK;")
	conn.execute(OPTIONS_TABLE)
	conn.close()

	options = []
	for folder in sorted(os.listdir(f"{NEW}/equities")):

		print("Processing Folder:", folder)

		if "options.csv" not in os.listdir(f"{NEW}/equities/{folder}"):
			continue

		options.append(pd.read_csv(f"{NEW}/equities/{folder}/options.csv"))

		if len(options) % 10 == 0:

			options = pd.concat(options)
			print("Indexing Options:", len(options))
			to_sql(options, "optionsBACK", engine)
			options = []

def index():

	index_instruments()
	index_rates()
	index_tickermaps()
	index_equities()
	index_options()	

if __name__ == "__main__":

	index()