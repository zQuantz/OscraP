from const import date_today, named_date_fmt, DIR
from bs4 import BeautifulSoup
from datetime import datetime
from threading import Thread
import pandas as pd
import numpy as np
import requests
import json, re
import sys, os
import joblib

headers_mobile = { 'User-Agent' : 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B137 Safari/601.1'}
START = "https://finance.yahoo.com/quote/{ticker}/options?p={ticker}"
SUMMARY = "https://finance.yahoo.com/quote/{ticker}/"
DIVIDENDS = "https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}"
PARSER = "lxml"

def fmt(str_number):
	return float(str_number.replace(',', '').replace('$', '').replace('%', '').replace('-', '0').replace('', '0'))

class Ticker(Thread):

	def __init__(self, ticker):

		Thread.__init__(self)
		self.stats = []
		self.ticker = ticker
		self.START = START.format(ticker = ticker)
		self.pages = [BeautifulSoup(requests.get(self.START, headers = headers_mobile).text, PARSER)]

	def initialize(self):

		## Current Price
		response = requests.get(SUMMARY.format(ticker = self.ticker), headers = headers_mobile)
		bs = BeautifulSoup(response.text, PARSER)
		price = bs.find("body").find("span", {"class" :' '.join(['Trsdu(0.3s)', 'Trsdu(0.3s)', 'Fw(b)', 'Fz(36px)', 'Mb(-4px)', 'D(b)'])}).text
		self.current_price = fmt(price)

		## Dividends
		div = bs.find("body").find("td", {"data-test" : "DIVIDEND_AND_YIELD-value"})
		try:
			if "N/A" not in div.text:
				self.div = float(re.search("\((.*?)\)", div.text)[0][1:-2]) / 100
			else:
				self.div = 0
		except Exception as e:
			print("Dividend Error. Setting to Zero.")
			self.div = 0

		## Expirations
		bs = self.pages[0]
		self.expiry_in_unix, self.expiry_as_string = list(zip(*[(option.get("value"), option.text) for option in bs.find_all("option")]))

	def scrape_options(self):

		for expiry, expiry_date in zip(self.expiry_in_unix, self.expiry_as_string):

			dt = datetime.fromtimestamp(int(expiry))
			expiry_date_fmt = datetime.strptime(expiry_date, named_date_fmt)
			expiration_days = np.busday_count(datetime.now().strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d"))

			page = self.START+f"&date={str(expiry)}"
			response = requests.get(page, headers = headers_mobile)
			bs = BeautifulSoup(response.text, PARSER)

			calls = bs.find("table", {"class" : "calls"})
			puts = bs.find("table", {"class" : "puts"})
			
			if calls:

				call_options = []
				for row in calls.find_all("tr")[1:]:
					es = [e for e in row.find_all("td")[2:]]
					self.stats.append([
							date_today,
							self.current_price,
							fmt(es[0].text),
							fmt(es[1].text),
							self.div,
							expiry_date_fmt,
							np.round(max(expiration_days / 252, 0), 6),
							'C',
							fmt(es[-1].text),
							fmt(es[2].text),
							fmt(es[3].text),
							fmt(es[-2].text),
							fmt(es[-3].text),

						])
			if puts:
			
				put_options = []
				for row in puts.find_all("tr")[1:]:
					es = [e for e in row.find_all("td")[2:]]
					self.stats.append([
							date_today,
							self.current_price,
							fmt(es[0].text),
							fmt(es[1].text),
							self.div,
							expiry_date_fmt,
							np.round(max(expiration_days / 252, 0), 6),
							'P',
							fmt(es[-1].text),
							fmt(es[2].text),
							fmt(es[3].text),
							fmt(es[-2].text),
							fmt(es[-3].text),

						])

		df = pd.DataFrame(self.stats, columns = ['CurrentDate', 'StockPrice', 'StrikePrice', 'OptionPrice', 'DividendYield',
											     'ExpierationDate', 'TimeToExpiry', 'OptionType', 'ImpliedVolatility', 'Bid',
											     'Ask', 'Volume', 'OpenInterest'])
		df.to_csv(f"{DIR}/option_data/{self.ticker}_{date_today}.csv", index=False)

	def run(self):

		self.initialize()
		self.scrape_options()
