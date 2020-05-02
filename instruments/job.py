from const import COLUMNS, DIR, CONFIG

from joblib import Parallel, delayed
from datetime import datetime
from bs4 import BeautifulSoup
from report import report
from index import index

import tarfile as tar
import pandas as pd
import sys, os

import string
import shutil
import time

sys.path.append(f"{DIR}/../utils")
from send_to_gcp import send_to_gcp
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
DATE = CONFIG['date']

EXCHANGES = [
	"American Stock Exchange",
	"NASDAQ Stock Exchange",
	"New York Stock Exchange",
	"Toronto Stock Exchange"
]

LETTERS = sorted(set(string.ascii_letters.lower()))
YAHOO_PROFILE = "https://ca.finance.yahoo.com/quote/{TICKER}/profile?p={TICKER}"
YAHOO_QUOTE = "https://ca.finance.yahoo.com/quote/{TICKER}/?p={TICKER}"
URL = "http://eoddata.com/{INDEX}/{EXCHANGE}/{SYMBOL}.htm"
FEATURES = "lxml"

SLEEP = 0.5
N_JOBS = 2

###################################################################################################

def parallel_log(msg):
	print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} - {msg}")

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
		parallel_log(','.join(exchanges[-1]))

	return exchanges

def get_sector_and_industry(ticker):

	url = YAHOO_PROFILE.format(TICKER=ticker)
	page = request(CONFIG, url)
	page = BeautifulSoup(page.content, features=FEATURES)

	if page.find("span", text="Fund Overview"):

		sector = page.find("span", text="Category").next_sibling.text
		industry = page.find("span", text="Fund Family").next_sibling.text
		return sector, industry, "ETF"

	elif page.find("span", text="Sector"):

		span = page.find("span", text="Sector")
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

	with open(f'{DIR}/instrument_data/{DATE}/{exchange_code}_log.log', 'a') as file:

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
				file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} - {log_entry}\n")

				time.sleep(SLEEP)

		df = pd.DataFrame(stats, columns = COLUMNS)
		df.to_csv(f'{DIR}/instrument_data/{DATE}/{exchange_code}_tickers.csv', index=False)

def main():

	parallel_log("Job Initiated.")

	parallel_log("Creating Directory.")
	os.mkdir(f"{DIR}/instrument_data/{DATE}")

	parallel_log("Parallel Jobs.")
	Parallel(n_jobs=2)(
		delayed(scrape)
		(exchange_code, exchange_name, '.TO' if exchange_code == 'TSX' else '')
		for exchange_code, exchange_name in get_exchanges()
	)

	parallel_log("Indexing.")
	df = index(parallel_log)

	parallel_log("Emailing.")
	report(df)

	###############################################################################################

	parallel_log("Storing.")
	try:

		df.to_csv(f"{DIR}/instrument_data/{DATE}/{DATE}.csv", index=False)

		with tar.open(f"{DIR}/instrument_data/{DATE}.tar.xz", "x:xz") as tar_file:
			for file in os.listdir(f"{DIR}/instrument_data/{DATE}"):
				tar_file.add(f"{DIR}/instrument_data/{DATE}/{file}", arcname=file)
			tar_file.add(f"{DIR}/err.log", arcname="err.log")
		
		send_to_gcp(BUCKET_PREFIX, BUCKET_NAME, f"{DATE}.tar.xz", f"{DIR}/instrument_data/")

		for folder in os.listdir(f"{DIR}/instrument_data"):
			if os.path.isdir(f"{DIR}/instrument_data/{folder}"):
				shutil.rmtree(f"{DIR}/instrument_data/{folder}")

	except Exception as e:

		parallel_log(f"Storage Error - {e}")

	###############################################################################################

	parallel_log("Job Terminated.")

if __name__ == '__main__':

	try:
		main()
	except Exception as e:
		parallel_log(f"Main Job Error - {e}")