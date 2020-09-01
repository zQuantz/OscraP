from const import CONFIG, DIR, OLD, NEW, TAR
from google.cloud import storage
import sqlalchemy as sql
import tarfile as tar
from tables import *
import pandas as pd
import sys, os
import json

###################################################################################################

NEWDIR = f"{DIR}/data/new"

db = CONFIG['db_address'].replace("finance", "test")
engine = sql.create_engine(db)

BUCKET = storage.Client().bucket(CONFIG["gcp_bucket_name"])

###################################################################################################

def to_sql(df, tablename, conn):
	df.to_sql(tablename, conn, if_exists="append", index=False, chunksize=100_000)

###################################################################################################

def download_data():

	os.mkdir(f"{DIR}/data/old")
	os.mkdir(f"{DIR}/data/new")
	os.mkdir(f"{DIR}/data/tar")
	os.mkdir(f"{DIR}/data/tar/old")
	os.mkdir(f"{DIR}/data/tar/new")

	FOLDERS = ["equities", "rates", "instruments", "rss"]
	for folder in FOLDERS:
		os.mkdir(f"{DIR}/data/old/{folder}")
		os.mkdir(f"{DIR}/data/tar/old/{folder}")
		if folder == "rates":
			folder = "treasuryrates"
		os.mkdir(f"{DIR}/data/new/{folder}")
		os.mkdir(f"{DIR}/data/tar/new/{folder}")

	for blob in BUCKET.list_blobs():

		if "/" not in blob.name:
			continue

		folder, filename = blob.name.split("/")
		filedate = filename.split(".")[0]

		if folder not in FOLDERS:
			continue

		modifier = ""
		if folder == "equities":
			modifier = f"/{filedate}/"
			os.mkdir(f"{DIR}/data/old/{folder}/{filedate}")
			os.mkdir(f"{DIR}/data/new/{folder}/{filedate}")

		print("Downloading:", folder, filename)
		blob.download_to_filename(f"{DIR}/data/tar/old/{folder}/{filename}")
		with tar.open(f"{DIR}/data/tar/old/{folder}/{filename}", "r:xz") as tar_file:
			tar_file.extractall(path=f"{DIR}/data/old/{folder}{modifier}")

def compress_data():

	for key in TAR:

		print("Processing Folder:", key)

		if key == "equity":

			for folder in sorted(os.listdir(NEW[key])):
				
				print("Compressing File:", folder)

				with tar.open(f"{TAR[key]}/{folder}.tar.xz", "x:xz") as tar_file:

					for file in os.listdir(f"{NEW[key]}/{folder}"):
						tar_file.add(f"{NEW[key]}/{folder}/{file}", file)

		else:

			for file in sorted(os.listdir(NEW[key])):

				print("Compressing File:", file)

				basename = file.split(".")[0]

				with tar.open(f"{TAR[key]}/{basename}.tar.xz", "x:xz") as tar_file:
					tar_file.add(f"{NEW[key]}/{file}", file)

###################################################################################################

def init_instruments():

	print("Instruments")

	instruments = []
	for file in sorted(os.listdir(f"{NEWDIR}/instruments")):
		instruments.append(pd.read_csv(f"{NEWDIR}/instruments/{file}", parse_dates=['last_updated']))
	
	instruments = pd.concat(instruments)
	instruments = instruments.sort_values(["last_updated", "market_cap"], ascending=[False, False])
	instruments = instruments.drop_duplicates(subset=["ticker", "exchange_code"], keep="first")

	print("Indexing Instruments")

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS instrumentsBACK;")
	conn.execute(INSTRUMENT_TABLE)
	to_sql(instruments, "instrumentsBACK", conn)
	conn.close()

def init_rates():

	print("Treasury Rates")

	treasuryrates = []
	for file in sorted(os.listdir(f"{NEWDIR}/treasuryrates")):
		treasuryrates.append(pd.read_csv(f"{NEWDIR}/treasuryrates/{file}", parse_dates=["date_current"]))
	
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
	for file in sorted(os.listdir(f"{NEWDIR}/treasuryratemap")):
		treasuryratemap.append(pd.read_csv(f"{NEWDIR}/treasuryratemap/{file}", parse_dates=["date_current"]))
	
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

def init_tickermaps():

	print("Ticker Maps")

	tickeroids, tickerdates = [], []
	for folder in sorted(os.listdir(f"{NEWDIR}/tickermaps")):
		tickeroids.append(pd.read_csv(f"{NEWDIR}/tickermaps/{folder}/tickeroids.csv"))
		tickerdates.append(pd.read_csv(f"{NEWDIR}/tickermaps/{folder}/tickerdates.csv"))

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

def init_equities():

	print("Equities Excl. Options")

	analysis, ohlc, keystats = [], [], []
	for folder in os.listdir(f"{NEWDIR}/equities"):

		files = os.listdir(f"{NEWDIR}/equities/{folder}")

		if "ohlc.csv" in files:
			ohlc.append(pd.read_csv(f"{NEWDIR}/equities/{folder}/ohlc.csv"))
		
		if "analysis.csv" in files:
			analysis.append(pd.read_csv(f"{NEWDIR}/equities/{folder}/analysis.csv"))
		
		if "keystats.csv" in files:
			keystats.append(pd.read_csv(f"{NEWDIR}/equities/{folder}/keystats.csv"))

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

def init_options():

	print("Options")

	conn = engine.connect()
	conn.execute("DROP TABLE IF EXISTS optionsBACK;")
	conn.execute(OPTIONS_TABLE)
	conn.close()

	options = []
	for folder in sorted(os.listdir(f"{NEWDIR}/equities")):

		print("Processing Folder:", folder)

		if "options.csv" not in os.listdir(f"{NEWDIR}/equities/{folder}"):
			continue

		options.append(pd.read_csv(f"{NEWDIR}/equities/{folder}/options.csv"))

		if len(options) % 10 == 0:

			options = pd.concat(options)
			print("Indexing Options:", len(options))
			to_sql(options, "optionsBACK", engine)
			options = []

def init():

	init_instruments()
	init_rates()
	init_tickermaps()
	init_equities()
	init_options()	

if __name__ == "__main__":

	init()