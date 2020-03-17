from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import sys, os

import string
import time

EXCHANGES = [
	"American Stock Exchange",
	"NASDAQ Stock Exchange",
	"New York Stock Exchange",
	"Toronto Stock Exchange"
]

headers_mobile = { 'User-Agent' : 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B137 Safari/601.1'}
YAHOO_BASE = "https://ca.finance.yahoo.com/quote/{ticker}/profile?p={ticker}"
URL = "{BASE}/{INDEX}/{EXCHANGE}/{SYMBOL}.htm"
BASE = "http://eoddata.com"
FEATURES = "lxml"
SLEEP = 0.1

LETTERS = sorted(set(string.ascii_letters.lower()))


def get_bs_obj(index, exchange, symbol):

	url = URL.format(BASE=BASE, INDEX=index, EXCHANGE=exchange, SYMBOL=symbol)
	page = requests.get(url)
	return BeautifulSoup(page.text, features=FEATURES)

def get_exchanges():

	page = get_bs_obj(index="stocklist", exchange="AMEX", symbol=LETTERS[0])
	select = page.find("select", {"id" : "ctl00_cph1_cboExchange"}).find_all("option")

	exchanges = []
	for option in select:

		if option.text not in EXCHANGES:
			continue

		exchanges.append((option.get_attribute_list("value")[0], option.text))
		print(exchanges[-1])

	return exchanges

def get_yahoo_finance_info(ticker):

	url = YAHOO_BASE.format(ticker=ticker)
	page = requests.get(url, headers=headers_mobile)
	page = BeautifulSoup(page.content, features=FEATURES)

	if page.find("span", text="Fund Overview"): ## This is an ETF

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

def main():

	stats = []
	for exchange_code, exchange_name in get_exchanges():

		for letter in LETTERS:

			page = get_bs_obj(index="stocklist", exchange=exchange_code, symbol=letter)
			rows = page.find("table", {"class" : "quotes"}).find_all("tr")[1:]

			if rows[0].find('td').text[0] != letter.upper():
				continue

			for row in rows:

				try:

					ticker = row.find('td').text
					name = row.find("td", text=ticker).next_sibling.text
					sector, industry, instrument_type = get_yahoo_finance_info(ticker)

					flag = 1

				except Exception as e:

					print(ticker, e)

					sector, industry, instrument_type = '', '', ''
					flag = 0

				stats.append([ticker, name, exchange_code, exchange_name, sector, industry, instrument_type, flag])
				print(stats[-1])

				time.sleep(SLEEP)

			df = pd.DataFrame(stats, columns = ['Ticker', 'Name', 'ExchangeCode', 'ExchangeName', 'Sector', 'Industry', 'InstrumentType', 'Flag'])
			df.to_csv('tmp/test.csv', index=False)

if __name__ == '__main__':

	main()
