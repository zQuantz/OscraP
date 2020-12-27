from const import CONFIG, DIR, OLD, NEW, TAR, _connector
from datetime import datetime, timedelta
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

	FOLDERS = ["equities", "treasuryrates", "instruments", "rss", "splits"]
	for folder in FOLDERS:

		os.mkdir(f"{DIR}/data/old/{folder}")
		os.mkdir(f"{DIR}/data/tar/old/{folder}")

		os.mkdir(f"{DIR}/data/new/{folder}")
		os.mkdir(f"{DIR}/data/tar/new/{folder}")

	for blob in BUCKET.list_blobs():

		if "/" not in blob.name:
			continue

		folder, filename = blob.name.split("/")
		filedate = filename.split(".")[0]

		if filename == "":
			continue

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

		return keystats

	for folder in sorted(OLD['equity'].iterdir()):

		print("Key Stats Core Transformation:", folder.name)

		file = folder / "keystats.csv"
		if not file.exists():
			print("No key stats file found.")
			continue
		
		keystats = pd.read_csv(file)
		keystats = transformation(keystats)
		keystats.to_csv(f"{NEW['equity']}/{folder.name}/keystats.csv", index=False)

def transform_analysis():

	def transformation(analysis):

		return analysis

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

		return rates

	for file in sorted(OLD['treasuryrates'].iterdir()):

		print("Rates Core Transformation:", file.name)
		
		rates = pd.read_csv(file)
		rates = transformation(rates)
		rates.to_csv(f"{NEW['treasuryrates']}/{file.name}", index=False)

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

def transform_splits():

	def transformation(splits):

		return splits

	for file in sorted(OLD['splits'].iterdir()):

		print("Splits Core Transofmration", file.name)

		splits = pd.read_csv(file)
		splits = transformation(splits)
		splits.to_csv(f"{NEW['splits']}/{file.name}", index=False)

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

	transform_splits()
	transform_instruments()
	transform_ohlc()
	transform_options()
	transform_analysis()
	transform_keystats()
	transform_rates()
	transform_rss()

###################################################################################################

def generate_split_series():

	splits = pd.concat([
		pd.read_csv(file)
		for file in NEW['splits'].iterdir()
	]).drop_duplicates()

	splits = splits[["ticker", "split_factor", "ex_date"]]
	splits['ex_date'] = pd.to_datetime(splits.ex_date) - timedelta(days=1)
	splits['ex_date'] = splits.ex_date.astype(str)

	dates = pd.date_range(start="2019-11-06", end=datetime.now(), freq="D")
	dates = dates.astype(str).values

	values = [
	    [
	        ticker,
	        date
	    ]
	    for ticker in splits.ticker.unique()
	    for date in dates
	]

	df = pd.DataFrame(values, columns=['ticker', 'date_current'])
	df = df.merge(splits, how="outer", left_on=["ticker", "date_current"], right_on=["ticker", "ex_date"])
	df = df[["ticker", "date_current", "split_factor"]]
	df = df.dropna(subset=["date_current"]).fillna(1)

	def by_ticker(ticker):
	    split_factor = ticker.split_factor.values
	    ticker['split_factor'] = split_factor[::-1].cumprod()[::-1]
	    return ticker

	return df.groupby("ticker").apply(by_ticker)

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

def init_equities(splits):

	def adjust_for_splits(ohlc):

		ohlc = ohlc.merge(splits,
						  on=["date_current", "ticker"],
						  how="outer")
		ohlc = ohlc.dropna(how="all", subset=ohlc.columns[2:-1])

		sf = ohlc.split_factor.fillna(1)
		ohlc['open_price'] = (ohlc.open_price * sf).round(2)
		ohlc['high_price'] = (ohlc.high_price * sf).round(2)
		ohlc['low_price'] = (ohlc.low_price * sf).round(2)
		ohlc['close_price'] = (ohlc.close_price * sf).round(2)
		ohlc['adjclose_price'] = (ohlc.adjclose_price * sf).round(2)
		ohlc['volume'] = (ohlc.volume / sf).round(0)

		return ohlc.drop(['split_factor'], axis=1)

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
	ohlc = adjust_for_splits(ohlc)

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

def init_options(splits):

	def adjust_for_splits(options):

		options = options.merge(splits,
						  on=["date_current", "ticker"],
						  how="outer")
		options = options.dropna(how="all", subset=options.columns[2:-1])

		sf = options.split_factor.fillna(1)
		options['strike_price'] = (options.strike_price * sf).round(2)
		options['bid_price'] = (options.bid_price * sf).round(2)
		options['option_price'] = (options.option_price * sf).round(2)
		options['ask_price'] = (options.ask_price * sf).round(2)
		options['volume'] = (options.volume / sf).round(0)
		options['open_interest'] = (options.open_interest / sf).round(0)

		oid = options.ticker + ' ' + options.expiration_date.astype(str)
		oid += ' ' + options.option_type
		sp = options.strike_price.round(2).astype(str)
		sp = sp.str.rstrip("0").str.rstrip(".")
		options['option_id'] = oid + sp

		return options.drop(["split_factor"], axis=1)

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
			options = adjust_for_splits(options)
			print("Indexing Options", len(options))
			_connector.write("optionsBACK", options)
			options = []

	if len(options) != 0:
		options = pd.concat(options)
		print("Final Index.\nIndexing Options", len(options))
		_connector.write("optionsBACK", options)

def init_splits():

	print("Initializing Splits")
	_connector.execute("DROP TABLE IF EXISTS stocksplitsBACK;")
	_connector.execute(STOCKSPLITS_TABLE)

	print("Initializing Tmp Splits")
	_connector.execute("DROP TABLE IF EXISTS stocksplitstmpBACK;")
	_connector.execute(STOCKSPLITSTMP_TABLE)

	print("Initializing Splits Status")
	_connector.execute("DROP TABLE IF EXISTS stocksplitstatusBACK;")
	_connector.execute(STOCKSPLITSTATUS_TABLE)

	splits = []
	for file in sorted((NEWDIR / "splits").iterdir()):
		splits.append(pd.read_csv(file))
	splits = pd.concat(splits).drop_duplicates()
	splits['processed_timestamp'] = datetime.now()

	_connector.write("stocksplitsBACK", splits)

def init():

	splits = generate_split_series()

	init_instruments()
	init_rates()
	init_equities(splits)
	init_options(splits)
	init_splits()

def main():

	# download_data()
	# transform()
	init()
	# compress_data()

if __name__ == "__main__":

	main()