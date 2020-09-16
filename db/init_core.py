from const import CONFIG, DIR, OLD, NEW, TAR, _connector
from google.cloud import storage
from pathlib import Path
import tarfile as tar
from tables import *
import pandas as pd
import sys, os

###################################################################################################

NEWDIR = Path(f"{DIR}/data/new")
BUCKET = storage.Client().bucket(CONFIG["gcp_bucket_name"])

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

			for folder in sorted(NEW[key].iterdir()):
				
				print("Compressing Equity Folder:", folder.name)

				with tar.open(f"{TAR[key]}/{folder.name}.tar.xz", "x:xz") as tar_file:

					for file in os.listdir(f"{NEW[key]}/{folder.name}"):
						tar_file.add(f"{NEW[key]}/{folder.name}/{file}", file)

		else:

			for file in sorted(NEW[key].iterdir()):

				print(f"Compressing {key.capitalize()} File:", file.name)

				basename = file.name.split(".")[0]
				
				with tar.open(f"{TAR[key]}/{basename}.tar.xz", "x:xz") as tar_file:
					tar_file.add(f"{NEW[key]}/{file.name}", file.name)

###################################################################################################

def transform_options():

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
		options.implied_volatility *= 100
		
		return options

	for folder in sorted(OLD['equity'].iterdir()):

		print("Options Core Transformation:", folder.name)

		file = folder / "options.csv"
		if not file.exists():
			print("No options file found.")
			continue
		
		options = pd.read_csv(file, parse_dates=["date_current", "expiration_date"])
		options = transformation(options)
		options.to_csv(f"{NEW['equity']}/{folder.name}/options.csv", index=False)

def transform_keystats():

	def transformation(keystats):

		keystats = keystats[~keystats.ticker.str.contains(".TO")]
		return keystats.dropna(subset=["value"])

	for folder in sorted(OLD['equity'].iterdir()):

		print("Key Stats Core Transformation:", folder.name)

		file = folder / "key_stats.csv"
		if not file.exists():
			print("No key stats file found.")
			continue
		
		keystats = pd.read_csv(file)
		keystats = transformation(keystats)
		keystats.to_csv(f"{NEW['equity']}/{folder.name}/keystats.csv", index=False)

def transform_analysis():

	def transformation(analysis):

		analysis = analysis[~analysis.ticker.str.contains(".TO")]
		return analysis.dropna(subset=["value"])

	for folder in sorted(OLD['equity'].iterdir()):

		print("Analysis Core Transformation:", folder.name)

		file = folder / "analysis.csv"
		if not file.exists():
			print("No analysis file found.")
			continue
		
		analysis = pd.read_csv(file)
		analysis = transformation(analysis)
		analysis.to_csv(f"{NEW['equity']}/{folder.name}/analysis.csv", index=False)

def transform_ohlc():

	def transformation(ohlc):

		ohlc = ohlc[~ohlc.ticker.str.contains(".TO")]
		ohlc['dividend_yield'] *= 100

		rename = {
			key : f"{key}_price"
			for key in ["open", "high", "low", "close"]
		}
		rename['stock_volume'] = 'volume'
		rename['adj_close'] = 'adjclose_price'
		ohlc = ohlc.rename(rename, axis=1)

		return ohlc

	for folder in sorted(OLD['equity'].iterdir()):

		print("OHLC Core Transformation:", folder.name)

		file = folder / "ohlc.csv"
		if not file.exists():
			print("No analysis file found.")
			continue
		
		ohlc = pd.read_csv(file)
		ohlc = transformation(ohlc)
		ohlc.to_csv(f"{NEW['equity']}/{folder.name}/ohlc.csv", index=False)

def transform_rates():

	def transformation(rates):

		if rates.shape[1] == 14:
			rates = rates.iloc[:, 1:]
		
		rates.loc[:, rates.columns[1:]] *= 100

		return rates

	for file in sorted(OLD['rates'].iterdir()):

		print("Rates Core Transformation:", file.name)
		
		rates = pd.read_csv(file)
		rates = transformation(rates)
		rates.to_csv(f"{NEW['rates']}/{file.name}", index=False)

def transform_instruments():

	def transformation(instruments):

		return instruments

	for file in sorted(OLD['instruments'].iterdir()):

		if "_" in file.name or ".log" in file.name:
			continue

		print("Instruments Core Transformation:", file.name)
		
		instruments = pd.read_csv(file)
		instruments = transformation(instruments)
		instruments.to_csv(f"{NEW['instruments']}/{file.name}", index=False)

def transform_rss():

	def transformation(rss):

		return rss

	for filename in sorted(OLD['rss'].iterdir()):

		print("RSS Core Transformation:", filename.name)

		with filename.open() as file:
			rss = file.read()

		rss = transformation(rss)

		with open(f"{NEW['rss']}/{filename.name}", "w") as file:
			file.write(rss)

def transform():

	print("Core Data Transformation")

	transform_ohlc()
	transform_options()
	transform_analysis()
	transform_keystats()
	transform_rates()
	transform_instruments()
	transform_rss()

###################################################################################################

def init_instruments():

	print("Initializing Instruments")

	instruments = []
	for file in sorted((NEWDIR / "instruments").iterdir()):
		instruments.append(pd.read_csv(file, parse_dates=['last_updated']))
	
	instruments = pd.concat(instruments)
	instruments = instruments.sort_values(["last_updated", "market_cap"], ascending=[False, False])
	instruments = instruments.drop_duplicates(subset=["ticker", "exchange_code"], keep="first")

	print("Indexing Instruments")
	print(instruments)

	_connector.execute("DROP TABLE IF EXISTS instrumentsBACK;")
	_connector.execute(INSTRUMENT_TABLE)
	_connector.write("instrumentsBACK", instruments)

def init_rates():

	print("Initializing Treasury Rates")

	treasuryrates = []
	for file in sorted((NEWDIR / "treasuryrates").iterdir()):
		treasuryrates.append(pd.read_csv(file, parse_dates=["date_current"]))
	
	treasuryrates = pd.concat(treasuryrates)
	treasuryrates = treasuryrates[treasuryrates.date_current >= "2019-01-01"]
	treasuryrates = treasuryrates.sort_values("date_current", ascending=True)

	print("Indexing Treasury Rates")
	print(treasuryrates)

	_connector.execute("DROP TABLE IF EXISTS treasuryratesBACK;")
	_connector.execute(TREASURYRATES_TABLE)
	_connector.write("treasuryratesBACK", treasuryrates)

def init_equities():

	print("Initializing Equities Excl. Options")

	analysis, ohlc, keystats = [], [], []
	for folder in sorted((NEWDIR / "equities").iterdir()):

		if (folder / "ohlc.csv").exists():
			ohlc.append(pd.read_csv(folder / "ohlc.csv"))
		
		if (folder / "analysis.csv").exists():
			analysis.append(pd.read_csv(folder / "analysis.csv"))
		
		if (folder / "keystats.csv").exists():
			keystats.append(pd.read_csv(folder / "keystats.csv"))

	ohlc = pd.concat(ohlc)
	analysis = pd.concat(analysis)
	keystats = pd.concat(keystats)

	###############################################################################################

	_connector.execute("DROP TABLE IF EXISTS ohlcBACK;")
	_connector.execute(OHLC_TABLE)
	
	_connector.execute("DROP TABLE IF EXISTS analysisBACK;")
	_connector.execute(ANALYSIS_TABLE)
	
	_connector.execute("DROP TABLE IF EXISTS keystatsBACK;")
	_connector.execute(KEYSTATS_TABLE)

	print("Indexing OHLC")
	print(ohlc)
	_connector.write("ohlcBACK", ohlc)

	print("Indexing Analysis")
	print(analysis)
	_connector.write("analysisBACK", analysis)

	print("Indexing Key Stats")
	print(keystats)
	_connector.write("keystatsBACK", keystats)

def init_options():


	print("Initializing Options")
	_connector.execute("DROP TABLE IF EXISTS optionsBACK;")
	_connector.execute(OPTIONS_TABLE)

	options = []
	for folder in sorted((NEWDIR / "equities").iterdir()):

		print("Processing Folder:", folder.name)

		file = folder / "options.csv"
		if not file.exists():
			continue

		options.append(pd.read_csv(file))

		if len(options) % 10 == 0:

			options = pd.concat(options)
			print("Indexing Options", len(options))
			_connector.write("optionsBACK", options)
			options = []

	options = pd.concat(options)
	print("Final Index.\nIndexing Options", len(options))
	_connector.write("optionsBACK", options)

def init():

	init_instruments()
	init_rates()
	init_equities()
	init_options()

def main():

	# download_data()
	# transform()
	# init()
	compress_data()

if __name__ == "__main__":

	main()