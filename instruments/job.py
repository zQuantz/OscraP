from const import COLUMNS, CONFIG, DIR, DATA, DATE, _connector, logger

from bs4 import BeautifulSoup
from sqlalchemy import text

import tarfile as tar
import pandas as pd
import sys, os

import string
import shutil
import time

sys.path.append(f"{DIR}/../utils")
from send_email import send_email
from gcp import send_to_bucket
from request import request

###################################################################################################

NUMBERS = ''.join([str(i) for i in range(10)])

CONVERTER = {"K" : 1_000}
CONVERTER["M"] = CONVERTER["K"] * 1_000
CONVERTER["B"] = CONVERTER["M"] * 1_000
CONVERTER["T"] = CONVERTER["B"] * 1_000
for key in CONVERTER.copy():
	CONVERTER[key.lower()] = CONVERTER[key]

BUCKET_PREFIX = CONFIG['gcp_bucket_prefix']
BUCKET_NAME = CONFIG['gcp_bucket_name']

EXCHANGES = [
	"American Stock Exchange",
	"NASDAQ Stock Exchange",
	"New York Stock Exchange"
]

LETTERS = sorted(set(string.ascii_letters.lower()))
YAHOO_PROFILE = "https://ca.finance.yahoo.com/quote/{TICKER}/profile?p={TICKER}"
YAHOO_QUOTE = "https://ca.finance.yahoo.com/quote/{TICKER}/?p={TICKER}"
URL = "http://eoddata.com/{INDEX}/{EXCHANGE}/{SYMBOL}.htm"
FEATURES = "lxml"

SLEEP = 0.5
N_JOBS = 2

###################################################################################################

def get_bs_obj(index, exchange, symbol):

	url = URL.format(INDEX=index, EXCHANGE=exchange, SYMBOL=symbol)
	page = request(CONFIG, url)
	return BeautifulSoup(page.content, features=FEATURES)

def get_exchanges():

	page = get_bs_obj(index="stocklist", exchange="AMEX", symbol=LETTERS[0])
	select = page.find("select", {"id" : "ctl00_cph1_cboExchange"}).find_all("option")

	exchanges = []
	for option in select:

		if option.text not in EXCHANGES:
			continue

		exchanges.append((option.get_attribute_list("value")[0], option.text))
		logger.info(','.join(exchanges[-1]))

	return exchanges

def get_sector_and_industry(ticker):

	url = YAHOO_PROFILE.format(TICKER=ticker)
	page = request(CONFIG, url)
	page = BeautifulSoup(page.content, features=FEATURES)

	if page.find("span", text="Fund Overview"):

		sector = page.find("span", text="Category").next_sibling.text
		industry = page.find("span", text="Fund Family").next_sibling.text
		return sector, industry, "ETF"

	elif page.find("span", text="Industry"):

		span = page.find("span", text=["Sector", "Sector(s)"])
		sibs = span.fetchNextSiblings()[0]
		sector = sibs.text

		span = page.find("span", text="Industry")
		sibs = span.fetchNextSiblings()[0]
		industry = sibs.text

		return sector, industry, "Equity"

	else:

		return None, None, None

def get_market_cap(ticker):

	url = YAHOO_QUOTE.format(TICKER=ticker)
	page = request(CONFIG, url)
	page = BeautifulSoup(page.content, features=FEATURES)

	if page.find("span", text="Net Assets"):

		span = page.find("span", text="Net Assets")
		span = span.findNext("span")
		value = span.text

	elif page.find("span", text="Market Cap"):

		span = page.find("span", text="Market Cap")
		span = span.findNext("span")
		value = span.text

	else:

		return 0

	if value[-1] in NUMBERS:
		value = value.replace(',', '')
		return float(value)

	if value == 'N/A':
		return 0

	return float(value[:-1]) * CONVERTER[value[-1]]

def scrape(exchange_code, exchange_name, modifier=''):

	stats = []
	for letter in LETTERS:

		page = get_bs_obj(index="stocklist", exchange=exchange_code, symbol=letter)
		rows = page.find("table", {"class" : "quotes"}).find_all("tr")[1:]

		if rows[0].find('td').text[0] != letter.upper():
			continue

		for row in rows:

			try:

				error = None

				ticker = row.find('td').text
				name = row.find("td", text=ticker).next_sibling.text

				ticker = ticker.replace('.', '-')
				ticker += modifier
				
				market_cap = get_market_cap(ticker)
				sector, industry, instrument_type = get_sector_and_industry(ticker)
			
			except Exception as e:

				market_cap = 0
				sector, industry, instrument_type = None, None, None
				error = e

			stats.append([
				ticker,
				name,
				exchange_code,
				exchange_name,
				sector,
				industry,
				instrument_type,
				round(market_cap, 3),
			])

			log_entry = list(map(str, stats[-1]))
			log_entry = ','.join(log_entry)
			log_entry += ',' if not error else ',' + str(error)
			logger.info(log_entry)

			time.sleep(SLEEP)

	df = pd.DataFrame(stats, columns = COLUMNS)
	df.to_csv(f'{DATA}/{exchange_code}_tickers.csv', index=False)

def index():

	dfs = []
	for file in DATA.iterdir():
		if '.log' in file.name:
			continue
		dfs.append(pd.read_csv(file))

	df = pd.concat(dfs, sort=False).dropna()
	df = df.sort_values('market_cap', ascending=False)
	df = df[df.market_cap >= 1_000]

	cols = ["ticker", "exchange_code"]
	df = df.drop_duplicates(subset=cols, keep="first")
	df['last_updated'] = DATE

	ticker_codes = df.ticker + ' ' + df.exchange_code
	ticker_codes = ticker_codes.values.tolist()

	query = text(f"""
		DELETE FROM
			instruments
		WHERE
			CONCAT(ticker, " ", exchange_code) in :ticker_codes
		"""
	)
	query = query.bindparams(ticker_codes=ticker_codes)
	_connector.execute(query)
	_connector.write("instruments", df)

	df = _connector.read("SELECT * FROM instruments;")
	df = df.sort_values('market_cap', ascending=False)
	df = df.reset_index(drop=True)

	return df

def store(df):

	try:

		df.to_csv(f"{DATA}/{DATE}.csv", index=False)

		with tar.open(f"{DATA}.tar.xz", "x:xz") as tar_file:
			for file in DATA.iterdir():
				tar_file.add(file, arcname=file.name)
			tar_file.add(f"{DIR}/log.log", arcname="log.log")
		
		send_to_bucket(BUCKET_PREFIX,
					   BUCKET_NAME,
					   f"{DATE}.tar.xz",
					   f"{DIR}/instrument_data",
					   logger)

		for folder in (DIR / "instrument_data").iterdir():
			if folder.is_dir():
				shutil.rmtree(folder)

	except Exception as e:

		logger.info(f"Storage Error - {e}")

def report(df):

	os.system(f"bash {DIR}/utils/truncate_log_file.sh")
	attachments = [
		{
			"ContentType" : "plain/text",
			"filename" : "instruments.log",
			"filepath" : f"{DIR}"
		}
	]

	send_email(config=CONFIG,
			   subject="Instrument Table Summary",
			   body=df.to_html(),
			   attachments=attachments,
			   logger=logger)

def main():

	logger.info("Job Initiated.")

	logger.info("Creating Directory.")
	DATA.mkdir()

	for exchange_code, exchange_name in get_exchanges():
		scrape(exchange_code, exchange_name, modifier)

	logger.info("Indexing.")
	df = index()

	logger.info("Emailing.")
	report(df)

	logger.info("Storing.")
	store(df)

	logger.info("Job Terminated.")

if __name__ == '__main__':

	try:
		main()
	except Exception as e:
		logger.info(f"Main Job Error - {e}")
